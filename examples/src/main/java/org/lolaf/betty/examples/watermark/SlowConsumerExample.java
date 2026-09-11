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
package org.lolaf.betty.examples.watermark;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * A server producing faster than its client consumes, throttled by {@code writeHighWatermark} and
 * {@code writeLowWatermark}.
 *
 * <p>Betty counts the bytes a session has accepted for writing but not yet put on the socket. Crossing
 * {@code writeHighWatermark} calls {@link IOEventsListener#onWatermarkEvent} with {@code true}, and falling back
 * under {@code writeLowWatermark} calls it with {@code false}. Each edge is reported once, which is what makes the
 * pair usable as a produce/pause switch: the producer here stops on the first and starts again on the second, so the
 * backlog oscillates between the two marks instead of growing until something breaks.
 *
 * <p>The client is slow on purpose. It reads for 20 ms a second and stops selecting the rest of the time through
 * {@link IOSession#pause(Deadline)}, which is what a consumer that cannot keep up looks like from the server's side:
 * its receive window closes, the server's socket stops accepting bytes, and the backlog the watermarks measure is
 * what is left holding them.
 *
 * <p>Without the watermarks the producer would keep borrowing buffers for data the socket cannot take, and the write
 * pool — then the heap behind it — is what would pay for the client being slow.
 */
@Slf4j
public final class SlowConsumerExample {

    private static final int CHUNK_SIZE = 16 * 1024;

    private static final int WRITE_HIGH_WATERMARK = 512 * 1024;

    private static final int WRITE_LOW_WATERMARK = 64 * 1024;

    /**
     * Small enough that the kernel stops taking bytes early, so the backlog builds in betty where it can be seen
     * rather than in the socket buffers where it cannot.
     */
    private static final int SOCKET_BUFFER_SIZE = 64 * 1024;

    /**
     * Both the task ring and the write pool have to hold every chunk the high watermark allows in flight
     * ({@code WRITE_HIGH_WATERMARK / CHUNK_SIZE}), or the producer blocks on a full ring and the watermark never
     * gets to be the thing that throttles it.
     */
    private static final int WRITES_IN_FLIGHT = 256;

    /** All the client is willing to consume per {@link #READ_INTERVAL}, which is what makes it the slow end. */
    private static final int READ_BUDGET = 256 * 1024;

    private static final Duration READ_INTERVAL = Duration.ofSeconds(1);

    private static final Duration REPORT_INTERVAL = Duration.ofSeconds(1);

    private static final Duration THROTTLED_PARK = Duration.ofMillis(1);

    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9097);

    private static final Deadline STOP_DEADLINE = Deadline.of(Duration.ofSeconds(1));

    private SlowConsumerExample() {
    }

    public static void main(String[] args) {
        ProducingListener producingListener = new ProducingListener();
        SlowConsumerListener slowConsumerListener = new SlowConsumerListener();

        Server server = ServerBuilder.builder()
                .id("watermark-server")
                .bindAddress(ADDRESS)
                .ioEventsListener(producingListener)
                .ioSettings(producerSettings())
                .build()
                .newInstance()
                .start();

        Client client = ClientBuilder.builder()
                .id("watermark-client")
                .connectAddress(ADDRESS)
                .ioEventsListener(slowConsumerListener)
                .ioSettings(consumerSettings())
                .build()
                .newInstance()
                .start();

        ScheduledExecutorService readBudgets = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "watermark-read-budgets"));
        readBudgets.scheduleAtFixedRate(() -> slowConsumerListener.renewBudget(client), READ_INTERVAL.toMillis(),
                READ_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "watermark-reporter"));
        Report report = new Report(producingListener, slowConsumerListener);
        reporter.scheduleAtFixedRate(report, REPORT_INTERVAL.toMillis(), REPORT_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            reporter.shutdownNow();
            readBudgets.shutdownNow();
            client.stop(STOP_DEADLINE);
            server.stop(STOP_DEADLINE);
            log.info("stopped");
        }, "watermark-shutdown"));

        log.info("high watermark {} KB, low watermark {} KB, client consuming {} KB every {} ms - Ctrl-C to stop",
                WRITE_HIGH_WATERMARK / 1024, WRITE_LOW_WATERMARK / 1024, READ_BUDGET / 1024, READ_INTERVAL.toMillis());
    }

    private static IOSettings producerSettings() {
        return IOSettings.builder()
                .writeHighWatermark(WRITE_HIGH_WATERMARK)
                .writeLowWatermark(WRITE_LOW_WATERMARK)
                .tasksRingBufferSize(WRITES_IN_FLIGHT)
                .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                        .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                .poolSize(WRITES_IN_FLIGHT)
                                .bufferSize(CHUNK_SIZE)
                                .build())
                        .build())
                .socketOptions(socketBufferOption(StandardSocketOptions.SO_SNDBUF))
                .build();
    }

    private static IOSettings consumerSettings() {
        return IOSettings.builder()
                .socketOptions(socketBufferOption(StandardSocketOptions.SO_RCVBUF))
                .build();
    }

    private static Map<SocketOption, Object> socketBufferOption(SocketOption<Integer> option) {
        return Collections.singletonMap(option, SOCKET_BUFFER_SIZE);
    }

    /** Fills a session's socket as fast as it will take bytes, and stops when told the backlog is too deep. */
    private static final class Producer implements Runnable {

        private final IOSession session;
        private final LongAdder bytesSent;
        private final AtomicBoolean throttled = new AtomicBoolean();
        private volatile boolean running = true;
        private long sequence;

        private Producer(IOSession session, LongAdder bytesSent) {
            this.session = session;
            this.bytesSent = bytesSent;
        }

        @Override
        public void run() {
            while (running && session.isStarted()) {
                if (throttled.get()) {
                    LockSupport.parkNanos(THROTTLED_PARK.toNanos());
                    continue;
                }
                ByteBuffer chunk = session.borrow(CHUNK_SIZE);
                chunk.putLong(++sequence);
                // the payload is not the point: send the whole buffer, not just the eight bytes written into it
                chunk.position(chunk.limit());
                bytesSent.add(chunk.position());
                session.send(chunk, true);
            }
        }

        private void throttle(boolean throttle) {
            throttled.set(throttle);
        }

        private boolean isThrottled() {
            return throttled.get();
        }

        private void stop() {
            running = false;
        }
    }

    /** Starts a producer per session and lets the watermark events drive it. */
    private static final class ProducingListener implements IOEventsListener {

        private final LongAdder bytesSent = new LongAdder();
        private Producer producer;

        @Override
        public void onConnected(IOSession session) {
            Producer sessionProducer = new Producer(session, bytesSent);
            session.setAttachment(sessionProducer);
            producer = sessionProducer;
            new Thread(sessionProducer, "watermark-producer").start();
            log.info("producing to {}", session.getSocketAddress());
        }

        @Override
        public void onWatermarkEvent(IOSession session, boolean highWatermarkReached, long bytesLeftToWrite) {
            Producer sessionProducer = session.getAttachment();
            sessionProducer.throttle(highWatermarkReached);
            log.info("{} watermark with {} KB in flight - producer {}",
                    highWatermarkReached ? "HIGH" : "LOW",
                    bytesLeftToWrite / 1024,
                    highWatermarkReached ? "throttled" : "resumed");
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            message.position(message.limit());
        }

        @Override
        public void onDisconnected(IOSession session) {
            Producer sessionProducer = session.getAttachment();
            if (sessionProducer != null) {
                sessionProducer.stop();
            }
            log.info("client gone");
        }
    }

    /** Consumes {@link #READ_BUDGET} per interval and stops selecting until the next one. */
    private static final class SlowConsumerListener implements IOEventsListener {

        private final LongAdder bytesRead = new LongAdder();
        private final AtomicInteger budget = new AtomicInteger(READ_BUDGET);

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            int read = message.remaining();
            bytesRead.add(read);
            message.position(message.limit());
            if (budget.addAndGet(-read) <= 0) {
                // pausing from the callback is allowed: on the IO thread the task runs inline rather than being queued
                session.pause(Deadline.immediate());
            }
        }

        private void renewBudget(Client client) {
            budget.set(READ_BUDGET);
            if (client.isConnected()) {
                client.getIOSession().resume();
            }
        }
    }

    /** One line a second: what was produced, what was consumed, and which side of the watermarks the producer is on. */
    private static final class Report implements Runnable {

        private final ProducingListener producingListener;
        private final SlowConsumerListener slowConsumerListener;
        private long lastBytesSent;
        private long lastBytesRead;

        private Report(ProducingListener producingListener, SlowConsumerListener slowConsumerListener) {
            this.producingListener = producingListener;
            this.slowConsumerListener = slowConsumerListener;
        }

        @Override
        public void run() {
            Producer producer = producingListener.producer;
            if (producer == null) {
                return;
            }
            long bytesSent = producingListener.bytesSent.sum();
            long bytesRead = slowConsumerListener.bytesRead.sum();
            log.info("produced {} KB/s, consumed {} KB/s, producer {}",
                    (bytesSent - lastBytesSent) / 1024,
                    (bytesRead - lastBytesRead) / 1024,
                    producer.isThrottled() ? "throttled" : "running");
            lastBytesSent = bytesSent;
            lastBytesRead = bytesRead;
        }
    }
}
