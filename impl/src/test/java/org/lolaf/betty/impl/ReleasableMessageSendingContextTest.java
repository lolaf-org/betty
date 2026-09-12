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
import org.lolaf.betty.api.io.ReleasableMessageSendingContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The contract is that a sending context is released once the write is over, and the failure path used not to
 * honour it: every cancelled or failed write leaked its context, which on a disconnect is every write still queued.
 */
class ReleasableMessageSendingContextTest extends AbstractTest {

    // The writes a disconnect cancels are released by the same path the failing builder below exercises, so they
    // are covered by construction rather than by a test that would have to race the disconnect.

    private static final String PAYLOAD = "hello";

    private static final class Context implements ReleasableMessageSendingContext {

        /**
         * A count rather than a flag: releasing a pooled context twice hands the same object to two messages, so
         * "released" is not the whole contract - "released once" is.
         */
        private final AtomicInteger releases = new AtomicInteger();

        @Override
        public void release() {
            releases.incrementAndGet();
        }
    }

    @Test
    void releasesTheContextWhenTheMessageIsSent() {
        setupTestEnvAndWaitForConnections();
        Context context = new Context();

        clientIOsession.send(ByteBuffer.wrap(PAYLOAD.getBytes()), context, false);

        await().untilAsserted(() -> assertThat(context.releases).hasValue(1));
    }

    @Test
    void releasesTheContextWhenTheBuilderFails() {
        setupTestEnvAndWaitForConnections();
        Context context = new Context();
        AtomicReference<Exception> reportedError = new AtomicReference<>();

        clientIOsession.send(failingBuilder(), context,
                (message, sendingError, sendingContext) -> reportedError.set(sendingError));

        await().untilAsserted(() -> assertThat(context.releases).hasValue(1));
        assertThat(reportedError.get()).isInstanceOf(IOException.class).hasMessage("cannot build");
    }

    /**
     * A protocol that stores or logs what it sent has to walk the buffer it is handed back, and rewinding it in place
     * is how that is done without allocating a duplicate. The write is over because the socket took every byte, so
     * where the callback leaves the position must not be able to turn a finished message into a half-written one -
     * which would send it a second time and hand the callback a context already back in its pool.
     */
    @Test
    void sendsOnceWhenTheCallbackRewindsTheBufferItIsHanded() {
        setupTestEnvAndWaitForConnections();
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        Context context = new Context();
        AtomicInteger callbacks = new AtomicInteger();

        // write mode, position on the last byte written: that is what send takes, and a wrapped buffer is not it
        clientIOsession.send(ByteBuffer.allocate(PAYLOAD.length()).put(PAYLOAD.getBytes()), context,
                (message, sendingError, sendingContext) -> {
                    callbacks.incrementAndGet();
                    message.position(0);
                }, false);

        assertSentExactlyOnce(receivedBytesCount, callbacks, context);
    }

    /**
     * The same, for the builder path: it is the one a protocol numbering its messages in send order uses, so it is
     * the one that rewinds.
     */
    @Test
    void sendsOnceWhenTheCallbackRewindsTheBufferABuilderBuilt() {
        setupTestEnvAndWaitForConnections();
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        Context context = new Context();
        AtomicInteger callbacks = new AtomicInteger();

        clientIOsession.send(payloadBuilder(), context,
                (message, sendingError, sendingContext) -> {
                    callbacks.incrementAndGet();
                    message.position(0);
                });

        assertSentExactlyOnce(receivedBytesCount, callbacks, context);
    }

    private void assertSentExactlyOnce(AtomicInteger receivedBytesCount, AtomicInteger callbacks, Context context) {
        await().untilAsserted(() -> assertThat(receivedBytesCount).hasValue(PAYLOAD.length()));
        // a resend is the next write cycle, so hold the assertion open rather than read it the instant it first passes
        await().during(Duration.ofMillis(500)).untilAsserted(() -> {
            assertThat(receivedBytesCount).hasValue(PAYLOAD.length());
            assertThat(callbacks).hasValue(1);
            assertThat(context.releases).hasValue(1);
        });
    }

    private static IOWriter.ByteBufferBuilder payloadBuilder() {
        return new IOWriter.ByteBufferBuilder() {

            @Override
            public ByteBuffer build() {
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
        };
    }

    private static IOWriter.ByteBufferBuilder failingBuilder() {
        return new IOWriter.ByteBufferBuilder() {

            @Override
            public ByteBuffer build() throws IOException {
                throw new IOException("cannot build");
            }

            @Override
            public boolean isPooledByteBuffer() {
                return false;
            }

            @Override
            public int getEstimatedByteBufferSize() {
                return 0;
            }
        };
    }
}
