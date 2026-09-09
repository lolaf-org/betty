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
package org.lolaf.betty.api.io;

import java.nio.ByteBuffer;

/**
 * A sending context that betty releases itself once the write it belongs to is over, so an application holding a
 * pooled resource per message does not have to track the write to free it. See
 * {@link IOWriter#send(ByteBuffer, Object, boolean)}, {@link IOWriter#send(IOWriter.ByteBufferBuilder, Object, IOWriter.MessageSentCallback)}
 * and {@link IOWriter#send(ByteBuffer, Object, IOWriter.MessageSentCallback, boolean)}
 */
public interface ReleasableMessageSendingContext {

    /**
     * Called exactly once per write, whether it succeeded or failed, and always after the callback, the future or
     * {@link IOEventsListener#onWriteFailure(IOSession, ByteBuffer)} has run - so a handler still sees a live
     * context.
     * <p>
     * Usually on the session's IO thread, but not always: a write still queued when the session is torn down is
     * cancelled and released by whichever thread performs the teardown, which is the stopping thread when the IO
     * thread cannot take it. So this must be safe to call from any thread.
     * <p>
     * A failure here is logged rather than propagated - releasing a resource must not be able to break the session.
     */
    void release();
}