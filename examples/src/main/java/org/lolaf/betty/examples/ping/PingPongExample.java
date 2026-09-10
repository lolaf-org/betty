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
package org.lolaf.betty.examples.ping;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.IdleStrategySelectStrategy;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A betty client and a betty server in one JVM, bouncing a ping between them once a second.
 *
 * <p>The client sends a 16-byte frame - a send timestamp and a sequence number - the server sends the same frame
 * straight back, and the client prints the round trip. Everything not needed to show that is left at its default:
 * no SSL, no buffer pool sizing, no select strategy. Run it with {@code java -jar examples/target/ping-example.jar}
 * and stop it with Ctrl-C.
 */
@Slf4j
public final class PingPongExample {

    /** A send timestamp and a sequence number, both {@code long}. */
    private static final int FRAME_SIZE = Long.BYTES + Long.BYTES;

    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9099);

    private static final Duration PING_INTERVAL = Duration.ofSeconds(1);

    private static final Deadline STOP_DEADLINE = Deadline.of(Duration.ofSeconds(1));

    private PingPongExample() {
    }

    public static void main(String[] args) {
        Server server = ServerBuilder.builder()
                .id("ping-server")
                .bindAddress(ADDRESS)
                .ioEventsListener(new PingServerListener())
                .ioSettings(IOSettings.builder().trackReceiveTime(true).build())
                .build()
                .newInstance()
                .start();

        Client client = ClientBuilder.builder()
                .id("ping-client")
                .connectAddress(ADDRESS)
                .ioEventsListener(new PingClientListener())
                .ioSettings(IOSettings.builder().trackReceiveTime(true).build())
                .build()
                .newInstance()
                .start();

        // The one-second wait happens here and never inside a callback: an IO thread that sleeps stops serving
        // every other session on that worker.
        ScheduledExecutorService pingScheduler = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "ping-scheduler"));
        pingScheduler.scheduleAtFixedRate(new PingSender(client), 0, PING_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            pingScheduler.shutdownNow();
            client.stop(STOP_DEADLINE);
            server.stop(STOP_DEADLINE);
            log.info("stopped");
        }, "ping-shutdown"));

        log.info("pinging {} every {}s - Ctrl-C to stop", ADDRESS, PING_INTERVAL.getSeconds());
    }

    /**
     * Writes one ping per tick, from the scheduler thread rather than from an IO thread. That is allowed on the
     * default settings, and only on those: {@code IOSettings.multiThreadedWriteAPICalls} is true and the write
     * buffer pool's {@code multiThreadingAccessMode} is {@code MULTIPLE_BORROWER_THREADS}. Narrow either one and
     * this send has to move onto the IO thread through {@link IOSession#processTask(Runnable)}.
     */
    private static final class PingSender implements Runnable {

        private final Client client;
        private long sequence;

        private PingSender(Client client) {
            this.client = client;
        }

        @Override
        public void run() {
            if (!client.isConnected()) {
                return;
            }
            IOSession session = client.getIOSession();
            ByteBuffer ping = session.borrow(FRAME_SIZE);
            ping.putLong(System.nanoTime()).putLong(++sequence);
            // true: the buffer came from the pool, so betty returns it there once the bytes are on the wire
            session.send(ping, true);
        }
    }

    /** Sends every frame it is given straight back. */
    private static final class PingServerListener implements IOEventsListener {

        @Override
        public void onConnected(IOSession session) {
            log.info("client connected from {}", session.getSocketAddress());
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            // TCP is a stream: a read can carry several frames, or end part way through one. Consume whole frames
            // only - betty compacts what is left unread into the front of the next read.
            while (message.remaining() >= FRAME_SIZE) {
                ByteBuffer pong = session.borrow(FRAME_SIZE);
                pong.putLong(message.getLong()).putLong(message.getLong());
                session.send(pong, true);
            }
        }

        @Override
        public void onDisconnected(IOSession session) {
            log.info("client disconnected");
        }
    }

    /** Reports the round trip of every frame that comes back. */
    private static final class PingClientListener implements IOEventsListener {

        @Override
        public void onConnected(IOSession session) {
            log.info("connected to {}", session.getSocketAddress());
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            while (message.remaining() >= FRAME_SIZE) {
                // `message` belongs to the pool and is recycled the moment this method returns: read it here,
                // never keep a reference to it
                long roundTripInNanos = System.nanoTime() - message.getLong();
                log.info("pong seq={} rtt={}us", message.getLong(), TimeUnit.NANOSECONDS.toMicros(roundTripInNanos));
            }
        }

        @Override
        public void onDisconnected(IOSession session) {
            log.info("disconnected");
        }
    }
}
