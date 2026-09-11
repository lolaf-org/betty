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
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOSettings;

import org.lolaf.ringos.Deadline;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * What {@code orderedWrites} decides: whether a message sent on the IO thread may leave ahead of one an application
 * thread queued earlier.
 *
 * <p>Both messages have to be in flight at once for the question to mean anything, which is what the read handler
 * waiting here is for. Holding the IO thread inside {@code onRead} is the one way to be sure the write queue is not
 * drained in between - it is the thread that would drain it - and it makes the interleaving the same on every run
 * rather than something the scheduler decides.
 */
class IOThreadSendOrderingTest extends AbstractTest {

    private static final int MESSAGE_SIZE = 8;

    @Test
    void aSendOnTheIOThreadLeavesFirstByDefault() {
        assertThat(receivedWhenBothAreInFlight(false)).isEqualTo("BBBBBBBBAAAAAAAA");
    }

    @Test
    void orderedWritesKeepTheOrderTheSendsWereMadeIn() {
        assertThat(receivedWhenBothAreInFlight(true)).isEqualTo("AAAAAAAABBBBBBBB");
    }

    @Test
    void aPausedSessionWritesNothingUntilItIsResumed() {
        PauseThenSendListener serverListener = new PauseThenSendListener();
        CollectingListener clientListener = new CollectingListener();
        setupTestEnvAndWaitForConnections(
                getTestClientBuilder().toBuilder().ioEventsListener(clientListener).build(),
                getTestServerBuilder().toBuilder().ioEventsListener(serverListener).build());

        clientIOsession.send(filled(clientIOsession, (byte) 'T'), true);

        // the send is made from the IO thread, where nothing but the pause stops it reaching the socket
        await().untilAsserted(() -> assertThat(serverListener.hasSent()).isTrue());
        await().pollDelay(Duration.ofMillis(300)).atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(clientListener.getReceived()).isEmpty());

        serverClientIOsession.resume();

        await().untilAsserted(() -> assertThat(clientListener.getReceived()).isEqualTo("PPPPPPPP"));
    }

    /**
     * Queues A from an application thread while the server's read handler is held inside {@code onRead}, then lets it
     * send B from the IO thread.
     */
    private String receivedWhenBothAreInFlight(boolean orderedWrites) {
        SendOnReadListener serverListener = new SendOnReadListener();
        CollectingListener clientListener = new CollectingListener();
        setupTestEnvAndWaitForConnections(
                getTestClientBuilder().toBuilder().ioEventsListener(clientListener).build(),
                getTestServerBuilder().toBuilder()
                        .ioEventsListener(serverListener)
                        .ioSettings(IOSettings.builder().orderedWrites(orderedWrites).build())
                        .build());

        clientIOsession.send(filled(clientIOsession, (byte) 'T'), true);
        await().untilAsserted(() -> assertThat(serverListener.isInsideOnRead()).isTrue());

        serverClientIOsession.send(filled(serverClientIOsession, (byte) 'A'), true);
        serverListener.letItSend();

        await().untilAsserted(() -> assertThat(clientListener.getReceived()).hasSize(2 * MESSAGE_SIZE));
        return clientListener.getReceived();
    }

    private static ByteBuffer filled(IOSession session, byte value) {
        ByteBuffer buffer = session.borrow(MESSAGE_SIZE);
        for (int written = 0; written < MESSAGE_SIZE; written++) {
            buffer.put(value);
        }
        return buffer;
    }

    /** Answers the first read with B, but not before the test has queued A. */
    private static class SendOnReadListener extends TestIOEventsListener {

        private final CountDownLatch queued = new CountDownLatch(1);
        private final AtomicBoolean insideOnRead = new AtomicBoolean();
        private final AtomicBoolean answered = new AtomicBoolean();

        boolean isInsideOnRead() {
            return insideOnRead.get();
        }

        void letItSend() {
            queued.countDown();
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            message.position(message.limit());
            if (!answered.compareAndSet(false, true)) {
                return;
            }
            insideOnRead.set(true);
            try {
                if (!queued.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("the test never queued its message");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            session.send(filled(session, (byte) 'B'), true);
        }
    }

    /** Pauses its own session and then answers, which a pause has to hold back until the session is resumed. */
    private static class PauseThenSendListener extends TestIOEventsListener {

        private final AtomicBoolean sent = new AtomicBoolean();

        boolean hasSent() {
            return sent.get();
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            message.position(message.limit());
            if (sent.get()) {
                return;
            }
            session.pause(Deadline.immediate());
            session.send(filled(session, (byte) 'P'), true);
            sent.set(true);
        }
    }

    private static class CollectingListener extends TestIOEventsListener {

        private final StringBuilder received = new StringBuilder();

        synchronized String getReceived() {
            return received.toString();
        }

        @Override
        public synchronized void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            while (message.hasRemaining()) {
                received.append((char) message.get());
            }
        }
    }
}
