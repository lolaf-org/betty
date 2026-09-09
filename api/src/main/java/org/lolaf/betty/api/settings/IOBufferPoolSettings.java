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
package org.lolaf.betty.api.settings;

import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOBufferPool;
import org.lolaf.betty.api.stats.IOBufferPoolStats;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.lolaf.ringos.rb.RingBufferFactory;

import java.nio.ByteBuffer;
import java.util.List;

import static org.lolaf.betty.api.settings.IOSettings.DEFAULT_TASKS_RING_BUFFER_SIZE;

/**
 * How a session's write buffers are pooled: how many, of what sizes, and how many threads may borrow them.
 * <p>
 * Sizing is a trade rather than a correctness matter - a pool too small still works, falling back to allocation -
 * so the zones are best set from what {@link IOBufferPoolStats} reports rather than guessed.
 */
@Getter
@Builder(toBuilder = true)
public class IOBufferPoolSettings implements InstanceProvider<IOBufferPool> {
    /**
     * Off-heap buffers, which is what lets the socket write without a copy.
     */
    @Builder.Default
    private final boolean directBuffers = true;

    /**
     * Default allocated IO buffer alignment for the zones, if not specified (0) will use L1 cache line size (usually 64 bytes), must be a multiple of 8
     */
    @Builder.Default
    private final int alignment = 0;

    /**
     * Pool multi threading access mode, see enum for more details, this setting will be set automatically depending on
     * {@link ServerBuilder#getIoSessionsSharedWriteIoBufferPoolSettings()} or {@link IOSettings#isMultiThreadedWriteAPICalls()} provided settings
     */
    @Builder.Default
    private MultiThreadingAccessMode multiThreadingAccessMode = MultiThreadingAccessMode.MULTIPLE_BORROWER_THREADS;

    /**
     * Pool zones
     */
    @Singular
    private List<IOBufferPoolZone> zones;

    /**
     * IOBufferPool stats provider
     */
    @Builder.Default
    private IOBufferPoolStats.IOBufferPoolStatsProvider ioBufferPoolStatsProvider = (ioSession, ioBufferPool) -> null;

    @Override
    public IOBufferPool newInstance() {
        return InstanceProvider.getSpiInstance(this, IOBufferPool.IOBufferPoolFactory.class);
    }

    /**
     * Which ring buffer a zone uses, and so what concurrency it is safe under. Set from the other settings rather
     * than by hand: a mode weaker than the actual access pattern corrupts the pool rather than slowing it.
     */
    @Getter
    public enum MultiThreadingAccessMode {

        /**
         * Only one thread will call {@link IOBufferPool#borrowByteBuffer(int)}, typically single thread writing to the same IO session
         */
        ONE_BORROWER_THREAD(RingBufferFactory.AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER),
        /**
         * Multiple threads will call {@link IOBufferPool#borrowByteBuffer(int)}, typically multiple threads writing to the same IO session
         */
        MULTIPLE_BORROWER_THREADS(RingBufferFactory.AccessType.MULTI_CONSUMER_SINGLE_PRODUCER),
        /**
         * Multiple threads will call {@link IOBufferPool#borrowByteBuffer(int)} and {@link IOBufferPool#returnByteBuffer(ByteBuffer)},
         * typically for a shared IOBufferPool pool usage only
         */
        MULTIPLE_BORROWER_AND_RETURN_TO_POOL_THREADS(RingBufferFactory.AccessType.MULTI_CONSUMER_MULTI_PRODUCER);

        private final RingBufferFactory.AccessType ringBufferType;

        MultiThreadingAccessMode(RingBufferFactory.AccessType ringBufferType) {
            this.ringBufferType = ringBufferType;
        }

    }

    /**
     * A fixed number of buffers of one size. A pool holds several zones so that a small message does not take a
     * large buffer.
     * <p>
     * A borrow is offered to the zones <em>in the order they were declared</em> and taken by the first whose
     * buffers are large enough, so they must be declared smallest first: a large zone declared ahead of a small
     * one serves every request and the small one is never used.
     */
    @Getter
    @Builder(toBuilder = true)
    public static class IOBufferPoolZone {
        /**
         * How many buffers the zone holds. Defaults to the tasks ring buffer size, which is the number of writes
         * that can be in flight at once and so the number of buffers they can need.
         */
        @Builder.Default
        private final int poolSize = DEFAULT_TASKS_RING_BUFFER_SIZE;
        /**
         * IO buffer size for the zone
         */
        @Builder.Default
        private final int bufferSize = 4 * 1024;
    }
}
