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

import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.assertThat;

class IOBufferPoolImplTest {
    IOBufferPoolImpl ioBufferPool;
    IOBufferPoolSettings.IOBufferPoolZone zone;
    IOBufferPoolSettings settings;

    @BeforeEach
    public void setup() {
        zone = IOBufferPoolSettings.IOBufferPoolZone.builder()
                .bufferSize(1024)
                .poolSize(32)
                .build();
        settings = IOBufferPoolSettings.builder()
                .zone(zone)
                .build();
        ioBufferPool = new IOBufferPoolImpl(settings).start(null);
    }

    @AfterEach
    public void shutdown() {
        ioBufferPool.stop();
    }

    @Test
    void testStartStop() {
        assertThat(ioBufferPool.isStarted()).isTrue();
        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize());
        ioBufferPool.stop();
        assertThat(ioBufferPool.isStarted()).isFalse();
        assertThat(ioBufferPool.getFreeBufferCount()).isZero();
    }

    /**
     * A session's pool is released by whichever of its disconnection and its IO worker's stop gets there first, and
     * neither can see what the other did - so the second stop has to be free rather than a second
     * {@code invokeCleaner} on memory that is already gone. The pool buffers here are direct, which is what makes
     * this the real path and not a heap no-op.
     */
    @Test
    void stoppingTwiceReleasesTheMemoryOnlyOnce() {
        assertThat(ioBufferPool.borrowByteBuffer(1024).isDirect()).as("the zone memory is direct").isTrue();

        ioBufferPool.stop();
        ioBufferPool.stop();

        assertThat(ioBufferPool.isStarted()).isFalse();
        assertThat(ioBufferPool.getFreeBufferCount()).isZero();
    }

    @Test
    void stoppingAPoolThatWasNeverStartedIsANoOp() {
        IOBufferPoolImpl neverStarted = new IOBufferPoolImpl(settings);

        neverStarted.stop();

        assertThat(neverStarted.isStarted()).isFalse();
    }

    /**
     * The guard latches on being started, not on having been stopped once, so a pool can still be brought back up.
     */
    @Test
    void aStoppedPoolCanBeStartedAgain() {
        ioBufferPool.stop();

        ioBufferPool.start(null);

        assertThat(ioBufferPool.isStarted()).isTrue();
        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize());
    }

    @Test
    void testNewByteBuffer() {
        ByteBuffer bb = ioBufferPool.newByteBuffer(32);
        assertThat(bb.capacity()).isEqualTo(32);
        assertThat(bb.isDirect()).isTrue();

        ioBufferPool.stop();

        ioBufferPool = new IOBufferPoolImpl(settings.toBuilder().directBuffers(false).build()).start(null);

        bb = ioBufferPool.newByteBuffer(34);
        assertThat(bb.capacity()).isEqualTo(34);
        assertThat(bb.isDirect()).isFalse();
    }

    @Test
    void testBorrowWithExhaustedPool() {
        for (int i = 0; i < zone.getPoolSize(); i++) {
            ioBufferPool.borrowByteBuffer(1024);
        }
        assertThat(ioBufferPool.getFreeBufferCount()).isZero();

        ByteBuffer bb = ioBufferPool.borrowByteBuffer(1024);
        assertThat(bb.capacity()).isEqualTo(1025);
        assertThat(ioBufferPool.getFreeBufferCount()).isZero();

        ioBufferPool.returnByteBuffer(bb);

        assertThat(ioBufferPool.getFreeBufferCount()).isZero();
    }

    @Test
    void testBorrowAndReturn() {
        ByteBuffer bb = ioBufferPool.borrowByteBuffer(1024);

        assertThat(bb.capacity()).isEqualTo(1024);
        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize() - 1);

        ioBufferPool.returnByteBuffer(bb);

        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize());
    }

    @Test
    void testBorrowOverSizeAndReturn() {
        ByteBuffer bb = ioBufferPool.borrowByteBuffer(1025);

        assertThat(bb.capacity()).isEqualTo(1025);
        assertThat(ioBufferPool.getOversizeCapacityMiss()).isEqualTo(1);
        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize());

        ioBufferPool.returnByteBuffer(bb);

        assertThat(ioBufferPool.getFreeBufferCount()).isEqualTo(zone.getPoolSize());
    }
}