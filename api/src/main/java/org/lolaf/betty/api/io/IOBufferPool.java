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

import org.lolaf.betty.api.settings.Factory;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;

import java.nio.ByteBuffer;

/**
 * Pre-allocated write buffers, so that sending a message costs no allocation on the IO thread.
 * <p>
 * A pool is bound to one session unless {@code ServerBuilder} names a shared one, in which case several IO threads
 * reach it and the zone configuration in {@link IOBufferPoolSettings} is what keeps that contention bounded.
 * <p>
 * Buffers are borrowed and returned, never garbage: {@link #borrowByteBuffer(int)} falls back to a fresh
 * allocation when no zone can satisfy the request rather than failing, and the miss is counted, so a pool sized
 * too small shows up in the statistics instead of in an exception.
 */
public interface IOBufferPool {

    /**
     * Unbinds the pool from its session and releases its zones.
     *
     * @return this pool, reusable by a later {@link #start(IOSession)}
     */
    IOBufferPool stop();

    /**
     * Binds the pool to a session, which is also what gives its statistics something to report against.
     *
     * @param ioSession the session that will borrow from it
     * @return this pool
     */
    IOBufferPool start(IOSession ioSession);

    /**
     * Whether the pool is bound to a session.
     *
     * @return false before the first {@link #start(IOSession)} and after {@link #stop()}
     */
    boolean isStarted();

    /**
     * Turns statistics collection on or off at runtime.
     *
     * @param enable true to collect; collection is work on the IO thread, which is why it is not always on
     */
    void toggleIOBufferPoolStats(boolean enable);

    /**
     * Allocates a buffer outside the pool, for a caller that will manage its lifetime itself.
     *
     * @param capacity the capacity needed
     * @return a new buffer, which must not be handed to {@link #returnByteBuffer(ByteBuffer)}
     */
    ByteBuffer newByteBuffer(int capacity);

    /**
     * Takes a buffer from the pool, allocating one if no zone can serve the request.
     *
     * @param capacity the capacity needed
     * @return a buffer, never null and never after a wait - an unservable request is met by allocating and
     *         counting the miss
     */
    ByteBuffer borrowByteBuffer(int capacity);

    /**
     * Gives a borrowed buffer back. A buffer the pool did not issue is ignored rather than rejected.
     *
     * @param byteBuffer the buffer to return
     */
    void returnByteBuffer(ByteBuffer byteBuffer);

    /**
     * How many buffers are available to borrow across every zone.
     *
     * @return the count at the moment of the call, which several IO threads may be changing
     */
    int getFreeBufferCount();

    /**
     * The SPI the implementation module registers, reached through {@link IOBufferPoolSettings}.
     */
    interface IOBufferPoolFactory extends Factory<IOBufferPool, IOBufferPoolSettings> {

    }
}
