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
package org.lolaf.betty.api.stats;

import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWriter;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.lolaf.betty.api.io.IOEventsListener;

import java.nio.ByteBuffer;

/**
 * Per-session measurements, collected on the IO thread by an implementation the application supplies.
 * <p>
 * Every method here is called from the session's IO thread and costs it directly, which is why the default
 * implementation is {@link VoidStats} and why {@link #enabledByDefault()} exists: an application pays for the
 * measurements it asks for, not for the ones the library could take.
 */
public interface IOStats {


    /**
     * Indicates if the stats are enabled by default or not, if not call {@link Client#enableIOStats(boolean)} or {@link Server#enableIOStats(boolean)}
     * to enable/disable it during runtime
     *
     * @return true to start collecting as soon as the session opens
     */
    default boolean enabledByDefault() {
        return true;
    }

    /**
     * The clock the library times an operation with, asked of the implementation rather than read directly.
     * <p>
     * That is what makes measurement optional at its real cost: an implementation that does not time an operation
     * returns a constant and no {@code System.nanoTime()} is ever called for it.
     *
     * @param operation what is about to be, or has just been, timed
     * @return a nanosecond reading, meaningful only against another reading for the same operation
     */
    long getTimeInNanos(Operation operation);

    /**
     * Time taken on a given session to call {@link IOEventsListener#onRead(IOSession, ByteBuffer, long)}
     *
     * @param ioSession                  the session that was read from
     * @param methodCallStartTimeInNanos the reading taken before the call, from {@link #getTimeInNanos(Operation)}
     */
    default void onIOEventsListenerOnReadCall(IOSession ioSession, long methodCallStartTimeInNanos) {

    }

    /**
     * Time taken on a given session to call {@link IOEventsListener#onWrite(IOSession, ByteBuffer)} or one of the provided {@link IOWriter.MessageSentCallback}
     * provided when doing an API {@link IOWriter#send(ByteBuffer, Object, IOWriter.MessageSentCallback, boolean)} call
     *
     * @param ioSession                  the session that was written to
     * @param messageSendingContext      the context passed to the send call, or null if none was
     * @param methodCallStartTimeInNanos the reading taken before the call, from {@link #getTimeInNanos(Operation)}
     */
    default void onIOEventsListenerOnWriteCallback(IOSession ioSession, Object messageSendingContext, long methodCallStartTimeInNanos) {

    }

    /**
     * Time taken on a given session to read from a socket
     *
     * @param ioSession                 the session that was read from
     * @param readenBytes               how many bytes came off the socket
     * @param localReadStartTimeInNanos the reading taken before the read, from {@link #getTimeInNanos(Operation)}
     */
    default void onSocketRead(IOSession ioSession, int readenBytes, long localReadStartTimeInNanos) {

    }

    /**
     * Time taken on a given session to write to a socket
     *
     * @param ioSession                  the session that was written to
     * @param writtenBytes               how many bytes went onto the socket
     * @param localWriteStartTimeInNanos the reading taken before the write, from {@link #getTimeInNanos(Operation)}
     */
    default void onSocketWrite(IOSession ioSession, int writtenBytes, long localWriteStartTimeInNanos) {

    }

    /**
     * Time taken on a given session to write a message, this includes the complete time, IOThread enqueueing (optional if writing directly form IO thread), socket writing, write callback processing
     *
     * @param ioSession                    the session the message went out on
     * @param sentMessage                  the message as it was written
     * @param messageSendingContext        the context passed to the send call, or null if none was
     * @param localSendingStartTimeInNanos the reading taken when the send was requested, from
     *                                     {@link #getTimeInNanos(Operation)}
     */
    default void onMessageSent(IOSession ioSession, ByteBuffer sentMessage, Object messageSendingContext, long localSendingStartTimeInNanos) {

    }

    /**
     * Event when a session is closed
     *
     * @param ioSession the session that has closed
     */
    default void onSessionClosed(IOSession ioSession) {

    }

    /**
     * Event when a session is opened
     *
     * @param ioSession the session that has opened
     */
    default void onSessionOpened(IOSession ioSession) {

    }

    /**
     * What a {@link #getTimeInNanos(Operation)} reading is being taken for, so an implementation can time some
     * operations and not others.
     */
    enum Operation {
        /**
         * Reading bytes off the socket.
         */
        IO_SOCKET_READ,
        /**
         * Writing bytes onto the socket.
         */
        IO_SOCKET_WRITE,
        /**
         * Running a task submitted to the session's IO thread.
         */
        IO_THREAD_TASK,
        /**
         * A whole message send, from the request to the write callback.
         */
        IO_MESSAGE_WRITE,
        /**
         * The application's {@code onRead} callback.
         */
        IO_EVENTS_LISTENER_ON_READ,
        /**
         * The application's write callback.
         */
        IO_EVENTS_LISTENER_ON_WRITE;
    }

    /**
     * Called once per session to decide what it collects, which is what lets one session be measured while the
     * rest are not.
     */
    interface IOStatsProvider {

        /**
         * Decides what a session about to open will collect.
         *
         * @param newIoSession the session being opened
         * @return the statistics for it; {@link VoidStats#getInstance()} to measure nothing
         */
        IOStats get(IOSession newIoSession);

    }

    /**
     * Measures nothing, and is the default. Its {@link #getTimeInNanos(Operation)} returns a constant, so a
     * session using it makes no clock call at all.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    class VoidStats implements IOStats {

        private static final long ZERO = 0L;

        private static final VoidStats INSTANCE = new VoidStats();

        /**
         * The shared instance; it holds no state, so there is no reason for a second.
         *
         * @return the singleton
         */
        public static VoidStats getInstance() {
            return INSTANCE;
        }

        @Override
        public boolean enabledByDefault() {
            return false;
        }

        @Override
        public long getTimeInNanos(Operation operation) {
            return ZERO;
        }
    }
}
