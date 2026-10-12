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
import org.lolaf.betty.api.io.IOWriter;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.ringos.Deadline;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A protocol holding what a builder reads must know when the builder outlives the send: queued on the IO thread, it
 * is told before {@code send} returns; built at once, or handed over from another thread, it is not.
 */
class ByteBufferBuilderOnQueuedTest extends AbstractTest {

    private static final String PAYLOAD = "hello";
    private static final IOWriter.MessageSentCallback<Object> IGNORED = (message, sendingError, sendingContext) -> {
    };

    private static final class CountingBuilder implements IOWriter.ByteBufferBuilder {

        private final AtomicInteger built = new AtomicInteger();
        private final AtomicInteger queued = new AtomicInteger();
        private volatile int builtWhenQueued = -1;

        @Override
        public ByteBuffer build() {
            built.incrementAndGet();
            return ByteBuffer.allocate(PAYLOAD.length()).put(PAYLOAD.getBytes());
        }

        @Override
        public boolean isPooledByteBuffer() {
            return false;
        }

        @Override
        public int getEstimatedByteBufferSize() {
            return PAYLOAD.length();
        }

        @Override
        public void onQueued() {
            builtWhenQueued = built.get();
            queued.incrementAndGet();
        }
    }

    @Test
    void notToldWhenBuiltAtOnceOnTheIOThread() {
        setupTestEnvAndWaitForConnections();
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        CountingBuilder builder = new CountingBuilder();

        clientIOsession.processTask(() -> clientIOsession.send(builder, null, IGNORED));

        await().untilAsserted(() -> assertThat(receivedBytesCount).hasValue(PAYLOAD.length()));
        assertThat(builder.queued).hasValue(0);
    }

    @Test
    void toldOnceBeforeSendReturnsWhenQueuedOnAPausedSession() {
        setupTestEnvAndWaitForConnections();
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        IOSessionImpl session = (IOSessionImpl) clientIOsession;
        session.pause(Deadline.immediate());
        CountingBuilder builder = new CountingBuilder();
        AtomicInteger queuedWhenSendReturned = new AtomicInteger(-1);

        session.processTask(() -> {
            session.send(builder, null, IGNORED);
            queuedWhenSendReturned.set(builder.queued.get());
        });

        await().until(() -> queuedWhenSendReturned.get() != -1);
        assertThat(queuedWhenSendReturned).hasValue(1);
        assertThat(builder.builtWhenQueued).isZero();
        session.resume();
        await().untilAsserted(() -> assertThat(receivedBytesCount).hasValue(PAYLOAD.length()));
        assertThat(builder.queued).hasValue(1);
    }

    @Test
    void notToldWhenHandedOverFromAnotherThread() {
        setupTestEnvAndWaitForConnections();
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        CountingBuilder builder = new CountingBuilder();

        clientIOsession.send(builder, null, IGNORED);

        await().untilAsserted(() -> assertThat(receivedBytesCount).hasValue(PAYLOAD.length()));
        assertThat(builder.queued).hasValue(0);
    }

    /**
     * A builder the full ring turns away is failed there and then, so it is never left waiting.
     */
    @Test
    void notToldWhenTheWriteRingIsFull() {
        setupTestEnvAndWaitForConnections();
        IOSessionImpl session = (IOSessionImpl) clientIOsession;
        session.pause(Deadline.immediate());
        AtomicReference<CountingBuilder> rejected = new AtomicReference<>();

        session.processTask(() -> {
            for (int i = 0; i <= IOSettings.DEFAULT_TASKS_RING_BUFFER_SIZE && rejected.get() == null; i++) {
                CountingBuilder builder = new CountingBuilder();
                session.send(builder, null, (message, sendingError, sendingContext) -> {
                    if (sendingError != null) {
                        rejected.set(builder);
                    }
                });
            }
        });

        await().untilAsserted(() -> assertThat(rejected.get()).isNotNull());
        assertThat(rejected.get().queued).hasValue(0);
    }
}
