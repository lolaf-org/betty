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
import org.lolaf.betty.benchmarks.impl.SlowConsumerServer;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;
import org.lolaf.ringos.threading.FastThreadLocalThread;
import org.openjdk.jmh.annotations.*;

import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * What a peer that stops reading costs the writer.
 *
 * <p>The other benchmarks talk to a server that drains as fast as it is fed, so their sockets never refuse a byte and
 * the partial write path is never taken. Here {@link SlowConsumerServer} pauses between reads, so the window closes
 * repeatedly and every invocation carries messages across write cycles: this is the benchmark to run when changing
 * how a refused write is handled - deferring to the next {@code OP_WRITE}, retrying first, or anything between.
 *
 * <p>One invocation hands the session 4 MB and waits for the last byte to reach the socket, so the score is the time
 * to push a payload through a socket that keeps saying no. The consumer sets the floor; what is being compared is how
 * much the writer adds to it, and `STOCK` against `LOW_LATENCY` is where that shows - a busy-spun selector notices the
 * window reopening on its next pass, a blocking one has to be woken.
 */
@Slf4j
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(value = 1, jvmArgsPrepend = {
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/jdk.internal.misc=ALL-UNNAMED",
        "-Xmx8g", "-Xms8g"})
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BackPressureBenchmark {

    private static final int MESSAGE_SIZE = 16 * 1024;

    private static final int MESSAGES_PER_INVOCATION = 256;

    /** The ring holds a whole payload, so the score is the drain and not the sender waiting for a slot. */
    private static final int WRITES_IN_FLIGHT = MESSAGES_PER_INVOCATION;

    /** Twice the ring: a borrow that finds the pool empty allocates, and that would land in the alloc profile. */
    private static final int IO_WRITE_BUFFER_POOL_SIZE = 2 * MESSAGES_PER_INVOCATION;

    private static final int SOCKET_SEND_BUFFER_SIZE = 32 * 1024;

    private static final Deadline ALL_SENT_DEADLINE = Deadline.of(Duration.ofSeconds(30));

    private static final Deadline STOP_DEADLINE = Deadline.of(Duration.ofSeconds(1));

    @Param({"STOCK", "LOW_LATENCY"})
    ClientSettings clientSettings;

    private SlowConsumerServer server;
    private Client client;
    private IOSession session;

    @Setup
    public void setup() {
        Affinity.setAffinity(AbstractClientBenchmark.JMH_CORE_AFFINITY);
        System.gc();
        server = new SlowConsumerServer();
        server.start();
        client = ClientBuilder.builder()
                .id("back-pressure-client")
                .connectAddress(SlowConsumerServer.CONNECT_ADDRESS)
                .ioEventsListener((ioSession, message, localReceiveTimeInNanos) -> message.position(message.limit()))
                .ioSettings(IOSettings.builder()
                        .tasksRingBufferSize(WRITES_IN_FLIGHT)
                        .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                        .poolSize(IO_WRITE_BUFFER_POOL_SIZE)
                                        .bufferSize(MESSAGE_SIZE)
                                        .build())
                                .build())
                        .socketOptions(Map.of(StandardSocketOptions.TCP_NODELAY, true,
                                StandardSocketOptions.SO_SNDBUF, SOCKET_SEND_BUFFER_SIZE))
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
        server.waitForConnection();
        // the server accepting is not the client being ready: betty connects on its IO thread and the session
        // appears after
        while (!client.isConnected() || client.getIOSession() == null) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
        }
        session = client.getIOSession();
    }

    @TearDown
    public void tearDown() {
        client.stop(STOP_DEADLINE);
        server.shutdown();
        log.info("Slow consumer read {} bytes", server.getBytesRead());
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    public void deliver4MBToASlowConsumer() {
        for (int message = 0; message < MESSAGES_PER_INVOCATION; message++) {
            ByteBuffer buffer = session.borrow(MESSAGE_SIZE);
            // the payload is not the point, only its size
            buffer.position(buffer.limit());
            session.send(buffer, true);
        }
        if (!session.waitForAllMessagesSent(ALL_SENT_DEADLINE)) {
            throw new IllegalStateException("payload was still not on the socket after " + ALL_SENT_DEADLINE);
        }
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
}
