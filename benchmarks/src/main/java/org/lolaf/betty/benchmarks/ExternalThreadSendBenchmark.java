/*
 * Copyright © 2024-2026 Lolaf.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lolaf.betty.benchmarks;

import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.IdleStrategySelectStrategy;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.ss.WakeupSelectStrategy;
import org.lolaf.betty.benchmarks.impl.AbstractClientBenchmark;
import org.lolaf.betty.benchmarks.impl.ClientSettings;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;
import org.lolaf.ringos.threading.FastThreadLocalThread;
import org.openjdk.jmh.annotations.*;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Sends from a thread other than the IO thread, the path every send of the other benchmarks skips: their betty client
 * only answers reads, from its IO thread. Here each message crosses the session ring to the IO thread, so this is the
 * benchmark to run when changing how a send from an application thread is queued.
 *
 * <p>The score is the application thread's cost of one send: an invocation queues 1024 messages of 16 bytes into a ring
 * with room for all of them, and the wait for the peer to receive them is left out of the timing. Timing it as well
 * would measure the kernel, since the IO thread makes one {@code write} syscall per message, about 3 us on loopback.
 */
@Slf4j
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsPrepend = {
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Xmx8g", "-Xms8g"})
@State(Scope.Benchmark)
public class ExternalThreadSendBenchmark {

    private static final int MESSAGE_SIZE = 16;

    private static final int MESSAGES_PER_INVOCATION = 1024;

    /** The smallest a pool buffer can be: sizes are a multiple of the cache line. */
    private static final int POOL_BUFFER_SIZE = 64;

    private static final InetSocketAddress SINK_ADDRESS = new InetSocketAddress("localhost", 9002);

    private static final Deadline STOP_DEADLINE = Deadline.of(Duration.ofSeconds(1));

    @Param({"STOCK", "LOW_LATENCY"})
    ClientSettings clientSettings;

    private SinkServer sink;
    private Client client;
    private IOSession session;
    private long bytesSent;

    @Setup
    public void setup() {
        Affinity.setAffinity(AbstractClientBenchmark.JMH_CORE_AFFINITY);
        System.gc();
        sink = new SinkServer();
        sink.start();
        client = ClientBuilder.builder()
                .id("external-thread-send-client")
                .connectAddress(SINK_ADDRESS)
                .ioEventsListener((ioSession, message, localReceiveTimeInNanos) -> message.position(message.limit()))
                .ioSettings(IOSettings.builder()
                        .tasksRingBufferSize(2 * MESSAGES_PER_INVOCATION)
                        .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                        .poolSize(4 * MESSAGES_PER_INVOCATION)
                                        .bufferSize(POOL_BUFFER_SIZE)
                                        .build())
                                .build())
                        .socketOptions(Map.of(StandardSocketOptions.TCP_NODELAY, true))
                        .build())
                .ioWorkersGroup(IOWorkersGroupSettings.builder()
                        .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder()
                                .threadFactory(PinnedIOThread::new)
                                .selectStrategy(selectStrategy(clientSettings))
                                .build())
                        .build()
                        .newInstance())
                .build()
                .newInstance()
                .start();
        sink.waitForConnection();
        while (!client.isConnected() || client.getIOSession() == null) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        session = client.getIOSession();
    }

    @TearDown
    public void tearDown() {
        client.stop(STOP_DEADLINE);
        sink.shutdown();
    }

    @Setup(Level.Invocation)
    public void waitForThePreviousInvocationToBeReceived() {
        while (sink.getBytesRead() < bytesSent) {
            Thread.onSpinWait();
        }
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.NANOSECONDS)
    @OperationsPerInvocation(MESSAGES_PER_INVOCATION)
    public void send16BytesFromAnApplicationThread() {
        for (int message = 0; message < MESSAGES_PER_INVOCATION; message++) {
            session.send(session.borrow(MESSAGE_SIZE).putLong(message).putLong(message), true);
        }
        bytesSent += (long) MESSAGES_PER_INVOCATION * MESSAGE_SIZE;
    }

    private static SelectStrategy selectStrategy(ClientSettings clientSettings) {
        return clientSettings == ClientSettings.LOW_LATENCY
                ? new IdleStrategySelectStrategy(BusySpinIdleStrategy.getInstance())
                : new WakeupSelectStrategy(1);
    }

    private static final class PinnedIOThread extends FastThreadLocalThread {

        private PinnedIOThread(Runnable target) {
            super(() -> {
                Affinity.setAffinity(AbstractClientBenchmark.IO_THREAD_CORE_AFFINITY);
                target.run();
            });
        }
    }

    private static final class SinkServer extends Thread {

        private final ServerSocket server;
        private final AtomicLong bytesRead = new AtomicLong();
        private volatile boolean running = true;
        private volatile boolean connected;

        private SinkServer() {
            setDaemon(true);
            setName("sink-server");
            try {
                server = new ServerSocket();
                server.bind(SINK_ADDRESS);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }

        private long getBytesRead() {
            return bytesRead.get();
        }

        private void waitForConnection() {
            while (!connected) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            }
        }

        private void shutdown() {
            running = false;
            try {
                server.close();
            } catch (IOException e) {
                // don't care
            }
            interrupt();
            try {
                join(TimeUnit.SECONDS.toMillis(5));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public void run() {
            byte[] chunk = new byte[64 * 1024];
            try (Socket socket = server.accept(); InputStream in = socket.getInputStream()) {
                connected = true;
                int read;
                while (running && (read = in.read(chunk)) >= 0) {
                    bytesRead.addAndGet(read);
                }
            } catch (IOException e) {
                if (running) {
                    log.info("Sink connection ended: {}", e.getMessage());
                }
            }
        }
    }
}
