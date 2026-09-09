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

import org.junit.jupiter.api.Test;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkerLoadBalancer;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;

import java.nio.channels.NetworkChannel;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

class IOWorkerMigrationConcurrentWritesTest extends AbstractTest {

    private static final byte[] PAYLOAD = "hello\n".getBytes();
    private static final int MSG_COUNT = 10_000;
    private static final int TRIGGER_AT = 5_000;
    private static final int EXPECTED_BYTES = MSG_COUNT * PAYLOAD.length;

    /**
     * Waits for the writer thread, bounded. A plain {@code join()} turns the failure this test exists to catch into a
     * build that hangs for ever rather than a failing test: when the migration leaves the session's write ring buffer
     * undrained the writer stays blocked in {@code offerBlocking}, which was observed wedging a full build for over an
     * hour. Bounded, the same condition fails here with a message naming it.
     */
    private static void joinWriterOrFail(Thread writer) throws InterruptedException {
        writer.join(Duration.ofSeconds(30).toMillis());
        assertThat(writer.isAlive())
                .as("writer thread still blocked sending, the session write ring buffer is not being drained")
                .isFalse();
    }

    @Test
    void migrationDuringConcurrentClientWritesLosesNoBytes() throws Exception {
        // Migrate the CLIENT-side session (no ':' in its id). This exercises the OP_WRITE preservation
        // path: writes enqueued during the migration window must end up on the new worker's selector.
        MigrationTriggerLoadBalancer lb = new MigrationTriggerLoadBalancer(s -> !s.getId().contains(":"));
        IOWorkersGroup shared = IOWorkersGroupSettings.builder()
                .id("migration-shared")
                .ioWorkerLoadBalancer(lb)
                .ioWorkersRebalanceInterval(Duration.ofMillis(50))
                .clearIoThreadGroups()
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(2).build())
                .build()
                .newInstance();

        ClientBuilder clientBuilder = getTestClientBuilder().toBuilder()
                .ioWorkersGroup(shared)
                .ioSettings(IOSettings.builder().multiThreadedWriteAPICalls(true).build())
                .build();
        ServerBuilder serverBuilder = getTestServerBuilder().toBuilder()
                .ioWorkersGroup(shared)
                .acceptorIoWorkerGroup(IOWorkersGroupSettings.builder().id("migration-acceptor").build().newInstance())
                .build();

        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder);

        AtomicInteger receivedBytes = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);

        Thread writer = new Thread(() -> {
            for (int i = 0; i < MSG_COUNT; i++) {
                clientIOsession.send(PAYLOAD);
                if (i == TRIGGER_AT) {
                    lb.armed.set(true);
                }
                if (i % 100 == 0) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                }
            }
        }, "test-writer");
        writer.start();
        joinWriterOrFail(writer);

        // Every byte the writer sent must eventually reach the server's read listener.
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(receivedBytes.get()).isEqualTo(EXPECTED_BYTES));

        // Confirm the migration we triggered actually executed end-to-end.
        assertThat(lb.migratedSession.get()).isSameAs(clientIOsession);
        IOWorker[] workers = shared.getIOWorkers().toArray(new IOWorker[0]);
        assertThat(((IOSessionImpl) clientIOsession).getSelectionKey().selector()).isEqualTo(workers[1].getSelector());
        assertThat(((IOSessionImpl) clientIOsession).hasActiveSelectionKey()).isTrue();
    }

    @Test
    void migrationOfServerSideSessionDuringConcurrentReadsLosesNoBytes() throws Exception {
        // Migrate the SERVER-side session (id contains ':'). Exercises the read-side migration window:
        // bytes arriving during the brief cancel/re-register gap must be kernel-buffered and delivered
        // via OP_READ on the new worker's selector after the channel is re-registered.
        MigrationTriggerLoadBalancer lb = new MigrationTriggerLoadBalancer(s -> s.getId().contains(":"));
        IOWorkersGroup shared = IOWorkersGroupSettings.builder()
                .id("migration-shared")
                .ioWorkerLoadBalancer(lb)
                .ioWorkersRebalanceInterval(Duration.ofMillis(50))
                .clearIoThreadGroups()
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(2).build())
                .build()
                .newInstance();

        ClientBuilder clientBuilder = getTestClientBuilder().toBuilder()
                .ioWorkersGroup(shared)
                .ioSettings(IOSettings.builder().multiThreadedWriteAPICalls(true).build())
                .build();
        ServerBuilder serverBuilder = getTestServerBuilder().toBuilder()
                .ioWorkersGroup(shared)
                .acceptorIoWorkerGroup(IOWorkersGroupSettings.builder().id("migration-acceptor").build().newInstance())
                .build();

        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder);

        AtomicInteger receivedBytes = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);

        Thread writer = new Thread(() -> {
            for (int i = 0; i < MSG_COUNT; i++) {
                clientIOsession.send(PAYLOAD);
                if (i == TRIGGER_AT) {
                    lb.armed.set(true);
                }
                if (i % 100 == 0) {
                    LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
                }
            }
        }, "test-writer");
        writer.start();
        joinWriterOrFail(writer);

        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(receivedBytes.get()).isEqualTo(EXPECTED_BYTES));

        assertThat(lb.migratedSession.get()).isSameAs(serverClientIOsession);
        IOWorker[] workers = shared.getIOWorkers().toArray(new IOWorker[0]);
        assertThat(((IOSessionImpl) serverClientIOsession).getSelectionKey().selector()).isEqualTo(workers[1].getSelector());
        assertThat(((IOSessionImpl) serverClientIOsession).hasActiveSelectionKey()).isTrue();
    }

    static class MigrationTriggerLoadBalancer implements IOWorkerLoadBalancer {
        final AtomicBoolean armed = new AtomicBoolean();
        final AtomicReference<IOSession> migratedSession = new AtomicReference<>();
        private final Predicate<IOSession> sessionPicker;

        MigrationTriggerLoadBalancer(Predicate<IOSession> sessionPicker) {
            this.sessionPicker = sessionPicker;
        }

        @Override
        public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
            return ioWorkers[0];
        }

        @Override
        public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
            if (!armed.compareAndSet(true, false)) {
                return List.of();
            }
            for (IOSession s : ioWorkers[0].getRegisteredSessions()) {
                if (sessionPicker.test(s)) {
                    migratedSession.set(s);
                    return List.of(new SessionMove(s, ioWorkers[1]));
                }
            }
            // Target session wasn't on workers[0] when we ran; re-arm and try next pass.
            armed.set(true);
            return List.of();
        }
    }

}
