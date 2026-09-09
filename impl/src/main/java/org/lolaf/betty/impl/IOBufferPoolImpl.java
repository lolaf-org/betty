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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.io.IOBufferPool;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.stats.IOBufferPoolStats;
import org.lolaf.ringos.unsafe.UnsafeOperations;
import org.lolaf.ringos.unsafe.UnsafeOperationsApi;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory;

import java.nio.ByteBuffer;
import java.util.function.Consumer;
import java.util.function.IntFunction;

@RequiredArgsConstructor
class IOBufferPoolImpl implements IOBufferPool {

    private final BuffersZone[] zones;
    private final BuffersZone singleZone;
    private final int zonesSize;
    private final IntFunction<ByteBuffer> ioBufferAllocator;
    private final IntFunction<ByteBuffer> pooledBufferBorrower;
    private final Consumer<ByteBuffer> pooledBufferReturn;
    private final IOBufferPoolStats.IOBufferPoolStatsProvider ioBufferPoolStatsProvider;
    private final Runnable runStats;
    @Getter
    private int oversizeCapacityMiss;
    private IOBufferPoolStats ioBufferPoolStats;
    private IOSession ioSession;


    IOBufferPoolImpl(IOBufferPoolSettings ioBufferPoolSettings) {
        if (ioBufferPoolSettings.getZones().isEmpty()) {
            throw new IllegalArgumentException("No IO buffer zones provided");
        }
        this.ioBufferAllocator = ioBufferPoolSettings.isDirectBuffers() ? ByteBuffer::allocateDirect : ByteBuffer::allocate;
        int unsafeApiAlignment = UnsafeOperationsApi.ifAvailableDoReturn(UnsafeOperations::getL1CacheLineSize, UnsafeOperations.DEFAULT_L1_CACHE_LINE_SIZE);
        int alignment = ioBufferPoolSettings.getAlignment() == 0 ? unsafeApiAlignment : ioBufferPoolSettings.getAlignment();
        this.zones = ioBufferPoolSettings.getZones().stream().map(s -> new BuffersZone(alignment, s, ioBufferAllocator, ioBufferPoolSettings.getMultiThreadingAccessMode())).toArray(BuffersZone[]::new);
        this.zonesSize = zones.length;
        this.singleZone = zonesSize == 1 ? zones[0] : null;
        this.pooledBufferBorrower = zonesSize == 1 ? this::borrowByteBufferForSingleZone : this::borrowByteBufferForMultipleZone;
        this.pooledBufferReturn = zonesSize == 1 ? this::returnByteBufferForSingleZone : this::returnByteBufferForMultipleZone;
        this.ioBufferPoolStatsProvider = ioBufferPoolSettings.getIoBufferPoolStatsProvider();
        this.runStats = this::runStats;
    }

    @Override
    public void toggleIOBufferPoolStats(boolean enable) {
        if (ioBufferPoolStats != null) {
            if (enable) {
                IOStatsScheduler.getInstance().register(runStats, ioBufferPoolStats.getStatsResolution());
            } else {
                IOStatsScheduler.getInstance().unregister(runStats);
            }
        }
    }

    private void runStats() {
        long statsTime = System.currentTimeMillis();
        statsTime -= statsTime % 1000;
        int localOversizeCapacityMiss = oversizeCapacityMiss;
        oversizeCapacityMiss = 0;
        int totalRequests = localOversizeCapacityMiss;
        int totalHits = 0;
        ioBufferPoolStats.startStatsCollection(ioSession, statsTime);
        for (int i = 0; i < zonesSize; i++) {
            totalHits += zones[i].getBufferPoolHit();
            totalRequests += zones[i].onStats(ioBufferPoolStats, ioSession);
        }
        ioBufferPoolStats.onHitRatio(ioSession, totalRequests, (double) totalHits / totalRequests);
        ioBufferPoolStats.onOversizeBufferMiss(ioSession, localOversizeCapacityMiss);
        ioBufferPoolStats.endStatsCollection(ioSession);
    }

    @Override
    public IOBufferPoolImpl stop() {
        if (!isStarted()) {
            return this;
        }
        if (ioBufferPoolStats != null) {
            IOStatsScheduler.getInstance().unregister(runStats);
        }
        this.ioSession = null;
        for (int i = 0; i < zonesSize; i++) {
            zones[i].stop();
        }
        return this;
    }

    @Override
    public IOBufferPoolImpl start(IOSession ioSession) {
        ioBufferPoolStats = ioBufferPoolStatsProvider.get(ioSession, this);
        this.ioSession = ioSession;
        if (ioBufferPoolStats != null && ioBufferPoolStats.enabledByDefault()) {
            IOStatsScheduler.getInstance().register(runStats, ioBufferPoolStats.getStatsResolution());
        }
        for (int i = 0; i < zonesSize; i++) {
            zones[i].start();
        }
        return this;
    }

    @Override
    public ByteBuffer newByteBuffer(int capacity) {
        return ioBufferAllocator.apply(capacity);
    }

    @Override
    public ByteBuffer borrowByteBuffer(int capacity) {
        return pooledBufferBorrower.apply(capacity);
    }

    private ByteBuffer borrowByteBufferForMultipleZone(int capacity) {
        for (int i = 0; i < zonesSize; i++) {
            ByteBuffer borrowed = zones[i].borrow(capacity);
            if (borrowed != null) {
                return borrowed;
            }
        }
        return onOverCapacityBufferAsked(capacity);
    }

    private ByteBuffer borrowByteBufferForSingleZone(int capacity) {
        ByteBuffer borrowed = singleZone.borrow(capacity);
        if (borrowed != null) {
            return borrowed;
        }
        return onOverCapacityBufferAsked(capacity);
    }

    private ByteBuffer onOverCapacityBufferAsked(int capacity) {
        oversizeCapacityMiss++;
        return newByteBuffer(capacity);
    }

    @Override
    public void returnByteBuffer(ByteBuffer byteBuffer) {
        pooledBufferReturn.accept(byteBuffer);
    }

    private void returnByteBufferForSingleZone(ByteBuffer byteBuffer) {
        singleZone.returnToPool(byteBuffer);
    }

    private void returnByteBufferForMultipleZone(ByteBuffer byteBuffer) {
        for (int i = 0; i < zonesSize; i++) {
            if (zones[i].returnToPool(byteBuffer)) {
                return;
            }
        }
    }

    @Override
    public boolean isStarted() {
        return zones[0].isStarted();
    }

    @Override
    public int getFreeBufferCount() {
        int availiable = 0;
        for (int i = 0; i < zonesSize; i++) {
            availiable += zones[i].available();
        }
        return availiable;
    }

    public static class IOBufferPoolFactoryImpl implements IOBufferPoolFactory {

        @Override
        public IOBufferPool newInstance(IOBufferPoolSettings settings) {
            return new IOBufferPoolImpl(settings);
        }
    }

    @Slf4j
    private static class BuffersZone {
        private final IntFunction<ByteBuffer> allocator;
        private final int ioBufferSize;
        private final RingBuffer<ByteBuffer> pool;
        private final ByteBuffer memory;
        private final int alignment;
        private int bufferPoolMiss;
        @Getter
        private int bufferPoolHit;
        private boolean started;

        BuffersZone(int alignment, IOBufferPoolSettings.IOBufferPoolZone setting, IntFunction<ByteBuffer> allocator, IOBufferPoolSettings.MultiThreadingAccessMode threadingAccess) {
            this.alignment = alignment;
            if (setting.getBufferSize() % alignment != 0) {
                throw new IllegalArgumentException("Buffer size " + setting.getBufferSize() + " must be a multiple of " + this.alignment);
            }
            this.pool = getPool(threadingAccess, setting.getPoolSize());
            this.allocator = allocator;
            this.ioBufferSize = setting.getBufferSize();
            this.memory = allocator.apply(setting.getBufferSize() * setting.getPoolSize() + alignment);
        }

        private static RingBuffer<ByteBuffer> getPool(IOBufferPoolSettings.MultiThreadingAccessMode threadingAccess, int poolSize) {
            // keep the i -> null trick, this will instruct the ringbuffer to actually avoid to set to null polled buffer array entry and make a small perfs gain
            return RingBufferFactory.build(threadingAccess.getRingBufferType(), poolSize, i -> null);
        }

        int onStats(IOBufferPoolStats ioBufferPoolStats, IOSession ioSession) {
            int localBufferPoolHit = bufferPoolHit;
            int localBufferPoolMiss = bufferPoolMiss;
            resetHitMissCounters();
            ioBufferPoolStats.onPooledBufferHits(ioSession, ioBufferSize, localBufferPoolHit);
            ioBufferPoolStats.onStarvedPoolBufferMiss(ioSession, ioBufferSize, localBufferPoolMiss);
            ioBufferPoolStats.onFreeBufferCount(ioSession, ioBufferSize, pool.getCapacity(), pool.getSize());
            return localBufferPoolHit + localBufferPoolMiss;
        }

        void resetHitMissCounters() {
            bufferPoolMiss = bufferPoolHit = 0;
        }

        void start() {
            if (started) {
                return;
            }
            ByteBuffer alignedMemory = memory.isDirect() ? memory.alignedSlice(alignment) : memory;
            int pos = 0;
            while (!pool.isFull()) {
                alignedMemory.position(pos);
                alignedMemory.limit(pos + ioBufferSize);
                ByteBuffer aligned = alignedMemory.slice();
                if (aligned.capacity() != ioBufferSize) {
                    throw new IllegalArgumentException("Unable to have an aligned bytebuffer, should not have happened " + aligned.capacity() + "!=" + ioBufferSize);
                }
                pool.offer(aligned);
                pos += ioBufferSize;
            }
            started = true;
        }

        boolean isStarted() {
            return started;
        }

        void stop() {
            if (!started) {
                return;
            }
            if (!pool.isFull()) {
                log.info("Potential IOBuffer leak, pool on stop is not full {}!={}", pool.getSize(), pool.getCapacity());
            }
            UnsafeOperationsApi.ifAvailableDo(uo -> uo.invokeCleanerIfNeeded(memory));
            pool.clear();
            resetHitMissCounters();
            started = false;
        }

        boolean returnToPool(ByteBuffer byteBuffer) {
            if (byteBuffer.capacity() != ioBufferSize) {
                return false;
            }
            return pool.offer(byteBuffer.clear());
        }

        int available() {
            return pool.getSize();
        }

        ByteBuffer borrow(int capacity) {
            if (capacity > ioBufferSize) {
                return null;
            }
            ByteBuffer b = pool.poll();
            if (b != null) {
                bufferPoolHit++;
                return b;
            }
            bufferPoolMiss++;
            // pool exhausted we increase capacity by one to make sure it won't be returned into the zone
            capacity++;
            return allocator.apply(capacity);
        }
    }
}