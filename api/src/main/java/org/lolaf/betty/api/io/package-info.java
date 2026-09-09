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
/**
 * A connection and everything an application does with it: {@link org.lolaf.betty.api.io.IOSession}, the
 * {@link org.lolaf.betty.api.io.IOEventsListener} callbacks it reports through, the
 * {@link org.lolaf.betty.api.io.IOWriter} it sends on, and the threads it runs on -
 * {@link org.lolaf.betty.api.io.IOWorker} and {@link org.lolaf.betty.api.io.IOWorkersGroup}.
 * <p>
 * Every callback here runs on the IO thread that owns the session, so what they do is what that thread costs.
 * The two rules that follow are the ones worth knowing before writing any of them: the {@code ByteBuffer} handed to
 * {@code onRead} belongs to the {@link org.lolaf.betty.api.io.IOBufferPool} and is recycled when the callback
 * returns, and a {@link org.lolaf.betty.api.io.ReleasableMessageSendingContext} is released by betty once its write
 * is over rather than by the application.
 */
package org.lolaf.betty.api.io;
