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
 * Sizes a session's read buffer when it is full of a message the listener could not consume yet. Called only then,
 * on the session's IO thread, so it costs nothing while messages fit.
 */
@FunctionalInterface
public interface ReadBufferGrowthStrategy {

    /**
     * Doubles the buffer, so that a large message costs a few reallocations at most.
     */
    ReadBufferGrowthStrategy DOUBLING = (unconsumed, capacity, maxReadBufferSize) ->
            (int) Math.min(2L * capacity, maxReadBufferSize);

    /**
     * @param unconsumed        the start of the incomplete message, read-only, so a strategy that knows the framing can
     *                          size the buffer for the whole message at once
     * @param capacity          the current capacity, below {@code maxReadBufferSize}
     * @param maxReadBufferSize the limit set by {@code IOSettings.maxReadBufferSize}
     * @return the new capacity; raised to what the next read needs (a whole record on TLS) and capped at
     * {@code maxReadBufferSize}
     */
    int grow(ByteBuffer unconsumed, int capacity, int maxReadBufferSize);
}
