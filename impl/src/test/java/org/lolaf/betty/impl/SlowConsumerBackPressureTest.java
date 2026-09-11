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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.ringos.Deadline;

import java.net.SocketOption;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A peer that stops reading closes its receive window, and the socket then takes part of a message or none of it. The
 * session has to survive that, keep the unwritten remainder, and hand the stream over unchanged once the peer reads
 * again - and the watermark pair has to report both edges so an application can stop producing in between.
 */
class SlowConsumerBackPressureTest extends AbstractTest {
    
    private static final int CHUNK_SIZE = 16 * 1024;

    private static final int CHUNKS = 32;

    private static final int WRITE_HIGH_WATERMARK = 256 * 1024;

    private static final int WRITE_LOW_WATERMARK = 32 * 1024;

    private static final int SOCKET_BUFFER_SIZE = 32 * 1024;

    /**
     * Every chunk has to fit in the ring and in the pool at once, or the sender blocks before the watermark fires.
     */
    private static final int WRITES_IN_FLIGHT = 64;

    private static void sendChunks(IOSession session) {
        long sequence = 0;
        for (int chunk = 0; chunk < CHUNKS; chunk++) {
            ByteBuffer buffer = session.borrow(CHUNK_SIZE);
            while (buffer.remaining() >= Long.BYTES) {
                buffer.putLong(sequence++);
            }
            session.send(buffer, true);
        }
    }

    private static IOSettings producerSettings() {
        return IOSettings.builder()
                .writeHighWatermark(WRITE_HIGH_WATERMARK)
                .writeLowWatermark(WRITE_LOW_WATERMARK)
                .tasksRingBufferSize(WRITES_IN_FLIGHT)
                .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                        .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                .poolSize(WRITES_IN_FLIGHT)
                                .bufferSize(CHUNK_SIZE)
                                .build())
                        .build())
                .socketOptions(socketBufferOption(StandardSocketOptions.SO_SNDBUF))
                .build();
    }

    private static IOSettings consumerSettings() {
        return IOSettings.builder()
                .socketOptions(socketBufferOption(StandardSocketOptions.SO_RCVBUF))
                .build();
    }

    private static Map<SocketOption, Object> socketBufferOption(SocketOption<Integer> option) {
        return Collections.singletonMap(option, SOCKET_BUFFER_SIZE);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void consumerThatStopsReadingThrottlesTheWriterAndKeepsTheSession(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        WatermarkListener serverListener = new WatermarkListener();
        SequenceCheckingListener clientListener = new SequenceCheckingListener();
        setupTestEnvAndWaitForConnections(
                clientBuilder.toBuilder().ioEventsListener(clientListener).ioSettings(consumerSettings()).build(),
                serverBuilder.toBuilder().ioEventsListener(serverListener).ioSettings(producerSettings()).build());

        clientIOsession.pause(Deadline.immediate());
        sendChunks(serverClientIOsession);

        await().untilAsserted(() -> assertThat(serverListener.getHighWatermarkEvents()).hasPositiveValue());
        assertThat(serverClientIOsession.isStarted()).isTrue();
        assertThat(clientListener.getBytesRead()).hasValueLessThan((long) CHUNKS * CHUNK_SIZE);

        clientIOsession.resume();

        await().untilAsserted(() -> assertThat(clientListener.getBytesRead()).hasValue((long) CHUNKS * CHUNK_SIZE));
        assertThat(clientListener.getCorruption()).isNull();
        await().untilAsserted(() -> assertThat(serverListener.getLowWatermarkEvents()).hasPositiveValue());
        assertThat(serverClientIOsession.isStarted()).isTrue();
    }

    @Getter
    private static class WatermarkListener extends TestIOEventsListener {

        private final AtomicInteger highWatermarkEvents = new AtomicInteger();
        private final AtomicInteger lowWatermarkEvents = new AtomicInteger();

        @Override
        public void onWatermarkEvent(IOSession session, boolean highWatermarkReached, long bytesLeftToWrite) {
            if (highWatermarkReached) {
                highWatermarkEvents.incrementAndGet();
            } else {
                lowWatermarkEvents.incrementAndGet();
            }
        }
    }

    /**
     * Reads the sequence the sender wrote and remembers the first value that is not the one due, which is what a
     * duplicated, dropped or reordered remainder would show up as.
     */
    @Getter
    private static class SequenceCheckingListener extends TestIOEventsListener {

        private final AtomicLong bytesRead = new AtomicLong();
        private volatile String corruption;
        private long expected;

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            int startPosition = message.position();
            while (message.remaining() >= Long.BYTES) {
                long value = message.getLong();
                if (value != expected && corruption == null) {
                    corruption = "expected " + expected + " but read " + value;
                }
                expected++;
            }
            // whatever is left is a partial value: betty compacts it into the front of the next read
            bytesRead.addAndGet(message.position() - startPosition);
        }
    }
}
