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

import org.lolaf.betty.api.settings.IOSettings;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

/**
 * Socket sending methods. The sending behavior depends on the thread calling the send(...) method
 * In case of a regular application thread, message to send will be put in a queue and processed by session IO thread as soon as possible.
 * In case of IO thread (typically as a response from a processed read in the IO thread), the message is written straight
 * to the socket without going through that queue, and so ahead of whatever other threads have put in it: ordering is
 * kept per sending thread, not between them. It is only ever held back by a message the socket has not finished taking.
 * {@link IOSettings#isOrderedWrites()} queues those sends as well, which gives the session one order whichever thread
 * writes to it, for a ring round trip per message.
 * Methods implementations are lock free by design and thread safe
 */
public interface IOWriter {

    /**
     * Borrows a ByteBuffer from the bytes buffer pool typically for write operation.
     *
     * @param capacity the capacity of the buffer.
     * @return a ByteBuffer to work with
     */
    ByteBuffer borrow(int capacity);

    /**
     * Return a ByteBuffer borrowed trough  {@link #borrow(int)} to the pool
     *
     * @param borrowed the byte buffer to return to the pool
     */
    void unborrow(ByteBuffer borrowed);

    /**
     * Allocates a new ByteBuffer,
     *
     * @param capacity the ByteBuffer desired capacity
     * @return a ByteBuffer to work with
     */
    ByteBuffer allocate(int capacity);

    /**
     * Sends a byte array, will use a Bytebuffer from the pool to send it
     * <p>
     * Called from the IO thread this writes straight to the socket, ahead of what other threads have queued,
     * unless {@link IOSettings#isOrderedWrites()} is set.
     *
     * @param message the message to send
     */
    void send(byte[] message);

    /**
     * Sends a ByteBuffer without any success or failure notifications
     * <p>
     * Called from the IO thread this writes straight to the socket, ahead of what other threads have queued,
     * unless {@link IOSettings#isOrderedWrites()} is set.
     *
     * @param message                the message to send
     * @param ioBufferPoolByteBuffer flag to indicate that the provided ByteBuffer is managed by the session
     *                               IOBuffer pool and has been retrieved by calling {@link #borrow(int)}, in such case
     *                               API user does NOT have to return the buffer to the pool after IO operation success or failure
     *                               by calling  {@link #unborrow(ByteBuffer)} as the implementation will do it transparently for the user
     */
    void send(ByteBuffer message, boolean ioBufferPoolByteBuffer);

    /**
     * Sends a ByteBuffer with a CompletableFuture as callback
     * <p>
     * Called from the IO thread this writes straight to the socket, ahead of what other threads have queued,
     * unless {@link IOSettings#isOrderedWrites()} is set.
     *
     * @param message                the message to send
     * @param messageSendingContext  a context object bound to the message sent or null if none required.
     *                               In case where the provided object is an instance of {@link ReleasableMessageSendingContext},
     *                               its release() method will be automatically called once the message has been fully sent
     * @param ioBufferPoolByteBuffer flag to indicate that the provided ByteBuffer is managed by the session
     *                               IOBuffer pool and has been retrieved by calling {@link #borrow(int)}, in such case
     *                               API user does NOT have to return the buffer to the pool after IO operation success or failure
     *                               by calling  {@link #unborrow(ByteBuffer)} as the implementation will do it transparently for the user
     * @param <C>                    the sending context's type
     * @return A completable future of when the message will be sent
     */
    <C> CompletableFuture<C> send(ByteBuffer message, C messageSendingContext, boolean ioBufferPoolByteBuffer);


    /**
     * Sends a ByteBuffer with a given callback, best memory friendly option as it induces not additional
     * CompletableFuture object creation compared to {@link #send(ByteBuffer, Object, boolean)}
     * <p>
     * Called from the IO thread this writes straight to the socket, ahead of what other threads have queued,
     * unless {@link IOSettings#isOrderedWrites()} is set.
     *
     * @param message                the message to send
     * @param messageSendingContext  a context object bound to the message sent or null if none required.
     *                               In case where the provided object is an instance of {@link ReleasableMessageSendingContext},
     *                               its release() method will be automatically called once the message has been fully sent
     * @param messageSentCallback    a callback to be called when the message will be sent
     * @param ioBufferPoolByteBuffer flag to indicate that the provided ByteBuffer is managed by the session
     *                               IOBuffer pool and has been retrieved by calling {@link #borrow(int)}, in such case
     *                               API user does NOT have to return the buffer to the pool after IO operation success or failure
     *                               by calling  {@link #unborrow(ByteBuffer)} as the implementation will do it transparently for the user
     * @param <C>                    the sending context's type
     */
    <C> void send(ByteBuffer message, C messageSendingContext, MessageSentCallback<C> messageSentCallback, boolean ioBufferPoolByteBuffer);

    /**
     * Sends a message generated by a {@link ByteBufferBuilder}, the {@link ByteBufferBuilder#build()} call is always done
     * in the IO thread shortly before sending the message to the remote session.
     * This API methods allows to ensure that protocols that send message with a sequence number can generate their next sequence number
     * within the IO thread and avoid any out of order messages sending
     * <p>
     * Called from the IO thread this writes straight to the socket, ahead of what other threads have queued,
     * unless {@link IOSettings#isOrderedWrites()} is set.
     *
     * @param byteBufferBuilder     the builder for the ByteBuffer to be sent to the remote IO session
     * @param messageSendingContext a context object bound to the message sent or null if none required.
     *                              In case where the provided object is an instance of {@link ReleasableMessageSendingContext},
     *                              its release() method will be automatically called once the message has been fully sent
     * @param messageSentCallback   a callback to be called when the message will be sent with the ByteBuffer generated by the builder
     * @param <C>                   the sending context's type
     */
    <C> void send(ByteBufferBuilder byteBufferBuilder, C messageSendingContext, MessageSentCallback<C> messageSentCallback);

    /**
     * Notified once a write has finished, and the allocation-free alternative to a {@link CompletableFuture}.
     *
     * @param <C> the sending context's type
     */
    interface MessageSentCallback<C> {

        /**
         * Called when the write operation has finished.
         * <p>
         * The buffer is lent for the duration of the call and may be read however the implementation likes: rewind it,
         * walk it, flip it. Its position and limit are restored on return, so a protocol that stores or logs what it
         * sent can read the message back in place rather than allocate a duplicate. Do not keep the buffer past the
         * call - a pooled one is returned to the pool immediately afterwards, and handed to the next message.
         *
         * @param message               the message that was sent or not, may be null if message was build by a {@link ByteBufferBuilder#build()} that failed
         * @param sendingError          the exception that occurred when sending the message or null if message has been sent
         * @param messageSendingContext the message sending context or null if non provided
         */
        void onMessageWriteCallback(ByteBuffer message, Exception sendingError, C messageSendingContext);
    }

    /**
     * Defers building a message until the IO thread is about to write it, which is what lets a protocol number its
     * messages in send order.
     */
    interface ByteBufferBuilder {

        /**
         * Builds the ByteBuffer to be sent over the wire, will be called only in the IO thread shortly before sending it on the wire
         *
         * @return A ByteBuffer with position set to the last written byte position (NOT position zero)
         * @throws IOException if the message cannot be built; the write is then reported as failed
         */
        ByteBuffer build() throws IOException;

        /**
         * Indicate that the ByteBuffer returned by the {@link #build()} call is managed by the session
         * IOBuffer pool and has been retrieved by calling {@link #borrow(int)}, in such case
         * API user does NOT have to return the buffer to the pool after IO operation success or failure
         * by calling {@link #unborrow(ByteBuffer)} as the implementation will do it transparently for the user
         *
         * @return true if {@link #build()} returns a pooled buffer
         */
        boolean isPooledByteBuffer();

        /**
         * Gives an estimation of the size of the generated byte buffer, required to estimate IO session pending messages bytes count to send
         * and correctly track watermark state defined by settings {@link IOSettings#getWriteLowWatermark()} and {@link IOSettings#getWriteHighWatermark()}
         *
         * @return and estimation of the generated byte buffer size
         */
        int getEstimatedByteBufferSize();
    }
}