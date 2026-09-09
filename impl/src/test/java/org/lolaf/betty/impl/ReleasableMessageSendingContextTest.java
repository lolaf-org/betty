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
import java.util.concurrent.atomic.AtomicBoolean;
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

    private static final class Context implements ReleasableMessageSendingContext {

        private final AtomicBoolean released = new AtomicBoolean();

        @Override
        public void release() {
            released.set(true);
        }
    }

    @Test
    void releasesTheContextWhenTheMessageIsSent() {
        setupTestEnvAndWaitForConnections();
        Context context = new Context();

        clientIOsession.send(ByteBuffer.wrap("hello".getBytes()), context, false);

        await().untilAsserted(() -> assertThat(context.released).isTrue());
    }

    @Test
    void releasesTheContextWhenTheBuilderFails() {
        setupTestEnvAndWaitForConnections();
        Context context = new Context();
        AtomicReference<Exception> reportedError = new AtomicReference<>();

        clientIOsession.send(failingBuilder(), context,
                (message, sendingError, sendingContext) -> reportedError.set(sendingError));

        await().untilAsserted(() -> assertThat(context.released).isTrue());
        assertThat(reportedError.get()).isInstanceOf(IOException.class).hasMessage("cannot build");
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
