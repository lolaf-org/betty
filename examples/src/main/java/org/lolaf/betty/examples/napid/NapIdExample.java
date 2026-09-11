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
package org.lolaf.betty.examples.napid;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.lb.NapIdLoadBalancer;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.ringos.Deadline;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * {@link NapIdLoadBalancer} over a real network adapter, printing the NAPI ID of every session so the placement can
 * be checked rather than assumed.
 *
 * <p>A NAPI ID names the NIC RX queue a socket's packets arrive on, so the balancer puts the sessions sharing a queue
 * on one IO worker. Packets that never cross a NIC have no queue and no ID: <b>two processes on the same host talk
 * over local delivery, whatever address they use</b>, so running both halves here reports {@code 0} and the balancer
 * falls back. Run the server on one machine and the clients on another to see real IDs:
 *
 * <pre>
 * host A: java -cp napid-example.jar org.lolaf.betty.examples.napid.NapIdExample server
 * host B: java -cp napid-example.jar org.lolaf.betty.examples.napid.NapIdExample client 10.0.0.7
 * </pre>
 *
 * <p>With no argument it runs both halves in this JVM, which shows the wiring and the {@code 0}. The worker count
 * follows the adapter's RX queue count, because that is how many distinct IDs its sessions can report.
 */
@Slf4j
public final class NapIdExample {

    /** A sequence number, enough to keep traffic flowing so the kernel has something to report an ID for. */
    private static final int FRAME_SIZE = Long.BYTES;

    private static final int PORT = 9098;

    private static final int CLIENT_COUNT = 4;

    private static final int MAX_IO_WORKERS = 4;

    private static final Duration PING_INTERVAL = Duration.ofSeconds(1);

    private static final Duration REPORT_INTERVAL = Duration.ofSeconds(5);

    private static final Duration REBALANCE_INTERVAL = Duration.ofSeconds(2);

    private static final Deadline STOP_DEADLINE = Deadline.of(Duration.ofSeconds(1));

    private NapIdExample() {
    }

    public static void main(String[] args) throws IOException {
        String mode = args.length == 0 ? "both" : args[0];
        Nic nic = Nic.firstPhysical();
        log.info("adapter {} at {} with {} RX queue(s)", nic.getName(), nic.getAddress().getHostAddress(), nic.getRxQueues());

        IOWorkersGroup ioWorkersGroup = napIdBalancedGroup(nic);
        List<Runnable> shutdownSteps = new ArrayList<>();

        if (!"client".equals(mode)) {
            InetSocketAddress bindAddress = new InetSocketAddress(nic.getAddress(), PORT);
            Server server = ServerBuilder.builder()
                    .id("napid-server")
                    .bindAddress(bindAddress)
                    .ioWorkersGroup(ioWorkersGroup)
                    .ioEventsListener(new EchoListener())
                    .build()
                    .newInstance()
                    .start();
            shutdownSteps.add(() -> server.stop(STOP_DEADLINE));
            log.info("listening on {} - connect from another host with: client {}", bindAddress, nic.getAddress().getHostAddress());
        }

        if (!"server".equals(mode)) {
            InetSocketAddress connectAddress = connectAddress(mode, nic, args);
            ScheduledExecutorService pingScheduler = Executors.newSingleThreadScheduledExecutor(
                    runnable -> new Thread(runnable, "napid-ping-scheduler"));
            for (int i = 0; i < CLIENT_COUNT; i++) {
                Client client = ClientBuilder.builder()
                        .id("napid-client-" + i)
                        .connectAddress(connectAddress)
                        .ioWorkersGroup(ioWorkersGroup)
                        .ioEventsListener(new CountingListener())
                        .build()
                        .newInstance()
                        .start();
                shutdownSteps.add(() -> client.stop(STOP_DEADLINE));
                pingScheduler.scheduleAtFixedRate(new PingSender(client), 0, PING_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
            }
            shutdownSteps.add(pingScheduler::shutdownNow);
            log.info("{} clients connecting to {}", CLIENT_COUNT, connectAddress);
        }

        if ("both".equals(mode)) {
            log.warn("both halves run here, so the packets never leave the machine and never reach {}'s RX queues: "
                    + "expect napId=0 and placement by the fallback balancer", nic.getName());
        }

        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
                runnable -> new Thread(runnable, "napid-reporter"));
        reporter.scheduleAtFixedRate(() -> report(ioWorkersGroup), REPORT_INTERVAL.toMillis(), REPORT_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);
        shutdownSteps.add(reporter::shutdownNow);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdownSteps.forEach(Runnable::run);
            log.info("stopped");
        }, "napid-shutdown"));
    }

    private static IOWorkersGroup napIdBalancedGroup(Nic nic) {
        int ioThreadCount = Math.max(2, Math.min(nic.getRxQueues(), MAX_IO_WORKERS));
        return IOWorkersGroupSettings.builder()
                .id("napid")
                .ioWorkerLoadBalancer(NapIdLoadBalancer.getInstance())
                // a session is placed at connect time, when the kernel may not have associated it with a queue yet;
                // rebalancing is what moves it once the ID appears
                .ioWorkersRebalanceInterval(REBALANCE_INTERVAL)
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder()
                        .name("napid-io")
                        .ioThreadCount(ioThreadCount)
                        .build())
                .build()
                .newInstance()
                .start();
    }

    private static InetSocketAddress connectAddress(String mode, Nic nic, String[] args) {
        if (!"client".equals(mode)) {
            return new InetSocketAddress(nic.getAddress(), PORT);
        }
        if (args.length < 2) {
            throw new IllegalArgumentException("usage: NapIdExample client <server host> [port]");
        }
        // any TCP server on another machine will do to see real NAPI IDs; it does not have to be the betty server
        int port = args.length > 2 ? Integer.parseInt(args[2]) : PORT;
        return new InetSocketAddress(args[1], port);
    }

    private static void report(IOWorkersGroup ioWorkersGroup) {
        for (IOWorker ioWorker : ioWorkersGroup.getIOWorkers()) {
            List<IOSession> sessions = ioWorker.getRegisteredSessions();
            if (sessions.isEmpty()) {
                log.info("{}: idle", ioWorker.getName());
                continue;
            }
            for (IOSession session : sessions) {
                log.info("{}: {} napId={}", ioWorker.getName(), session.getId(), napIdOf(session));
            }
        }
    }

    private static String napIdOf(IOSession session) {
        try {
            return String.valueOf(NapIdLoadBalancer.napIdOf(session.getNetworkChannel()));
        } catch (IOException e) {
            return "unavailable";
        }
    }

    /** Writes one frame per tick, from the scheduler thread rather than from an IO thread. */
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
            ByteBuffer frame = session.borrow(FRAME_SIZE);
            frame.putLong(++sequence);
            session.send(frame, true);
        }
    }

    /** Sends every frame it is given straight back, so both ends keep receiving. */
    private static final class EchoListener implements IOEventsListener {

        @Override
        public void onConnected(IOSession session) {
            log.info("accepted {} on {}", session.getSocketAddress(), Thread.currentThread().getName());
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            while (message.remaining() >= FRAME_SIZE) {
                ByteBuffer echo = session.borrow(FRAME_SIZE);
                echo.putLong(message.getLong());
                session.send(echo, true);
            }
        }

        @Override
        public void onDisconnected(IOSession session) {
            log.info("disconnected {}", session.getSocketAddress());
        }
    }

    /** Drains what comes back; the report is what this example is here to show. */
    private static final class CountingListener implements IOEventsListener {

        @Override
        public void onConnected(IOSession session) {
            log.info("connected to {} on {}", session.getSocketAddress(), Thread.currentThread().getName());
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            message.position(message.limit());
        }
    }
}
