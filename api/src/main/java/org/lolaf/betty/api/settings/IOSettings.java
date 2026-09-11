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

import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWriter;
import lombok.Builder;
import lombok.Getter;

import java.net.SocketOption;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

/**
 * Per-socket settings, applied to every session unless {@code IOEventsListener.getIOSettings} returns something
 * else for a particular peer.
 */
@Getter
@Builder(toBuilder = true)
public class IOSettings {
    /**
     * Default for both the tasks ring buffer and a buffer pool zone, so the two stay in step unless deliberately
     * separated - a zone smaller than the ring starves under the very burst the ring exists to absorb.
     */
    public static final int DEFAULT_TASKS_RING_BUFFER_SIZE = 32;
    /**
     * Byte buffer allocated size when reading data from the socket, a too small value will not allow to read large messages
     * from network in one single call by the IO selector
     */
    @Builder.Default
    private final int readBufferSize = 8 * 1024;

    /**
     * Is the read buffer a direct buffer or a normal one
     */
    @Builder.Default
    private final boolean readDirectBuffer = true;

    /**
     * IO thread tasks ring buffer size for socket writes, when full the thread trying to write an IO message will busy spin until an element is free.
     * This size should be fine-tuned for high outgoing throughput applications writing messages using a thread
     * other than the IO thread to avoid message sending latency increase due to buffer being full.
     * If you modify the ring buffer size, {@link IOBufferPoolSettings.IOBufferPoolZone#getPoolSize()} should be adapted to the same size
     * to avoid pooled ByteBuffer misses due to pool starvation
     */
    @Builder.Default
    private final int tasksRingBufferSize = DEFAULT_TASKS_RING_BUFFER_SIZE;

    /**
     * Flag to indicate that application will use multiple threads when calling write operation on the IO session trough {@link IOWriter#send(byte[])}
     * or {@link IOWriter#send(ByteBuffer, boolean)} API calls, when disabled a more efficient queue can be used for enqueueing write operations.
     */
    @Builder.Default
    private final boolean multiThreadedWriteAPICalls = true;

    /**
     * The maximum number of bytes that can be written to a socket by the IO worker thread during a write cycle, allowing IO bandwidth fairness amongst connections.
     * Can add more latency to connections using a lot of bandwidth
     */
    @Builder.Default
    private final int maxBytesCountPerWriteCycle = Integer.MAX_VALUE;

    /**
     * In flight bytes to be written to socket to trigger a high watermark event {@link IOEventsListener#onWatermarkEvent(IOSession, boolean, long)}
     * set it to zero along with writeLowWatermark to disable it
     * <p>
     * Advisory: a send is never refused for being over it. A peer that stops reading leaves its messages queued and
     * the socket takes them when it can, so this pair is how a producer learns to stop until the backlog has drained
     * to {@link #writeLowWatermark}.
     */
    @Builder.Default
    private final int writeHighWatermark = 1024 * 1024 * 32;

    /**
     * In flight bytes to be written to socket to trigger a low watermark event {@link IOEventsListener#onWatermarkEvent(IOSession, boolean, long)}
     * set it to zero along with writeHighWatermark to disable it
     */
    @Builder.Default
    private final int writeLowWatermark = 8 * 1024;
    /**
     * Write buffer pool settings for the IO session
     */
    @Builder.Default
    private final IOBufferPoolSettings writeIoBufferPoolSettings = IOBufferPoolSettings.builder()
            .zone(IOBufferPoolSettings.IOBufferPoolZone.builder().build()).build();

    /**
     * Socket options for the session
     */
    @Builder.Default
    private final Map<SocketOption, Object> socketOptions = Collections.emptyMap();

    /**
     * Indicate that local message receive time in nanos should be tracked when calling {@link IOEventsListener#onRead(IOSession, ByteBuffer, long)}}
     * This will have however a very small impact in memory allocation and latency especially with a busy spin type selector
     */
    @Builder.Default
    private final boolean trackReceiveTime = false;

}
