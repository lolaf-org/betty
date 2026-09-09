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
package org.lolaf.betty.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.*;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.NetworkChannel;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class IOWorkersGroupImplTest {
    IOWorkersGroupImpl ioWorkersGroup;

    private static IOEventsListener mockEventsListener() {
        IOEventsListener mock = mock(IOEventsListener.class);
        when(mock.isReadyToConnect(any())).thenReturn(true);
        doAnswer(inv -> {
            ByteBuffer bb = inv.getArgument(1);
            bb.position(bb.limit());
            return null;
        }).when(mock).onRead(any(IOSession.class), any(ByteBuffer.class), anyLong());
        return mock;
    }

    @BeforeEach
    void setup() {
        ioWorkersGroup = new IOWorkersGroupImpl(IOWorkersGroupSettings.builder().build());
    }

    @AfterEach
    void shutdown() {
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
    }

    @Test
    void testMultipleTreadWithinGroup() {
        ioWorkersGroup = new IOWorkersGroupImpl(IOWorkersGroupSettings.builder().clearIoThreadGroups()
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(5).build()).build());

        ioWorkersGroup.start();

        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());
    }

    @Test
    void testMultipleStart() {
        assertThat(ioWorkersGroup.isStarted()).isFalse();

        ioWorkersGroup.start();

        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());

        ioWorkersGroup.start();

        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());
    }

    @Test
    void testMultipleStopAndRestart() {
        ioWorkersGroup.start();
        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());

        ioWorkersGroup.start();
        ioWorkersGroup.start();
        ioWorkersGroup.start();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());


        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        assertThat(ioWorkersGroup.isStarted()).isTrue();
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        assertThat(ioWorkersGroup.isStarted()).isTrue();
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        assertThat(ioWorkersGroup.isStarted()).isTrue();

        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        assertThat(ioWorkersGroup.isStarted()).isFalse();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isZero();

        ioWorkersGroup.start();
        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());

        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        assertThat(ioWorkersGroup.isStarted()).isFalse();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isZero();
    }

    /**
     * A group is reference counted, and the threads belong to whichever connector stops last. Until then a
     * {@code stop} is a no-op with the IO threads left running - a caller reading it as "the IO threads are done"
     * would be releasing resources under them, which is why the alive count is asserted between the stops and not
     * only after the last one.
     */
    @Test
    void aSharedGroupKeepsItsThreadsRunningUntilItsLastUserStops() {
        ioWorkersGroup.start();
        ioWorkersGroup.start();

        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());

        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount())
                .as("the second user still has its IO threads")
                .isEqualTo(ioWorkersGroup.getIOWorkersCount());

        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());

        assertThat(ioWorkersGroup.isStarted()).isFalse();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isZero();
    }

    /**
     * Stopping more often than the group was started must not push the reference count negative, which would leave a
     * later start unable to ever stop again.
     */
    @Test
    void stoppingMoreOftenThanStartedDoesNotBreakTheNextStart() {
        ioWorkersGroup.start();
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());
        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());

        ioWorkersGroup.start();

        assertThat(ioWorkersGroup.isStarted()).isTrue();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isEqualTo(ioWorkersGroup.getIOWorkersCount());

        ioWorkersGroup.stop(org.lolaf.ringos.Deadline.immediate());

        assertThat(ioWorkersGroup.isStarted()).isFalse();
        assertThat(ioWorkersGroup.getAliveIOWorkersCount()).isZero();
    }

    @Test
    void rebalanceSchedulerNotCreatedWhenIntervalIsNull() throws InterruptedException {
        CountingLoadBalancer lb = new CountingLoadBalancer();
        ioWorkersGroup = new IOWorkersGroupImpl(IOWorkersGroupSettings.builder()
                .ioWorkerLoadBalancer(lb)
                .clearIoThreadGroups()
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(2).build())
                .build());
        ioWorkersGroup.start();
        // Without ioWorkersRebalanceInterval, the scheduler does not run rebalance().
        Thread.sleep(200);
        assertThat(lb.rebalanceCalls.get()).isZero();
    }

    @Test
    void transparentMigrationMovesSessionToTargetWorker() {
        RecordingLoadBalancer lb = new RecordingLoadBalancer();
        IOWorkersGroup shared = IOWorkersGroupSettings.builder()
                .id("rebalance-shared")
                .ioWorkerLoadBalancer(lb)
                .ioWorkersRebalanceInterval(Duration.ofMillis(100))
                .clearIoThreadGroups()
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(2).build())
                .build()
                .newInstance();

        IOEventsListener serverEvents = mockEventsListener();
        IOEventsListener clientEvents = mockEventsListener();
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

        Server server = null;
        Client client = null;
        try {
            int port = TestPorts.findFree();
            server = ServerBuilder.builder()
                    .id("rebalance-server")
                    .bindAddress(new InetSocketAddress("localhost", port))
                    .ioEventsListener(serverEvents)
                    .ioWorkersGroup(shared)
                    .acceptorIoWorkerGroup(IOWorkersGroupSettings.builder().id("rebalance-acceptor").build().newInstance())
                    .build()
                    .newInstance()
                    .start();

            client = ClientBuilder.builder()
                    .id("rebalance-client")
                    .connectAddress(new InetSocketAddress("localhost", port))
                    .ioEventsListener(clientEvents)
                    .ioWorkersGroup(shared)
                    .scheduledExecutorService(scheduler)
                    .build()
                    .newInstance()
                    .start();

            IOWorker[] workers = shared.getIOWorkers().toArray(new IOWorker[0]);

            // Both connections (client side + server-accepted side) land on workers[0] because the LB
            // always selects workers[0]. The RecordingLoadBalancer.rebalance picks one and moves it.
            await().atMost(Duration.ofSeconds(5)).until(() -> !lb.emitted.isEmpty());

            IOWorker target = lb.emitted.get(0).getTarget();
            IOSession migrated = lb.emitted.get(0).getSession();

            await().atMost(Duration.ofSeconds(5)).until(() -> target.getRegisteredSessions().contains(migrated));

            for (IOWorker other : workers) {
                if (other != target) {
                    assertThat(other.getRegisteredSessions()).doesNotContain(migrated);
                }
            }
            assertThat(((IOSessionImpl) migrated).getSelectionKey().selector()).isEqualTo(target.getSelector());
            assertThat(((IOSessionImpl) migrated).getOwnerWorker()).isSameAs(target);
            assertThat(((IOSessionImpl) migrated).hasActiveSelectionKey()).isTrue();
        } finally {
            if (client != null) {
                client.stop(org.lolaf.ringos.Deadline.immediate());
            }
            if (server != null) {
                server.stop(org.lolaf.ringos.Deadline.immediate());
            }
            scheduler.shutdownNow();
        }
    }

    static class CountingLoadBalancer implements IOWorkerLoadBalancer {
        final AtomicInteger rebalanceCalls = new AtomicInteger();

        @Override
        public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
            return ioWorkers[0];
        }

        @Override
        public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
            rebalanceCalls.incrementAndGet();
            return List.of();
        }
    }

    static class RecordingLoadBalancer implements IOWorkerLoadBalancer {
        final List<SessionMove> emitted = new ArrayList<>();
        volatile boolean armed = true;

        @Override
        public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
            return ioWorkers[0];
        }

        @Override
        public synchronized List<SessionMove> rebalance(IOWorker[] ioWorkers) {
            if (!armed) {
                return List.of();
            }
            for (IOWorker w : ioWorkers) {
                if (!w.getRegisteredSessions().isEmpty()) {
                    IOSession s = w.getRegisteredSessions().get(0);
                    IOWorker target = (w == ioWorkers[0]) ? ioWorkers[1] : ioWorkers[0];
                    SessionMove move = new SessionMove(s, target);
                    emitted.add(move);
                    armed = false;
                    return List.of(move);
                }
            }
            return List.of();
        }
    }
}