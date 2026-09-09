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

import org.lolaf.betty.api.BaseBuilder;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.nio.channels.NetworkChannel;
import java.util.function.BiConsumer;

/**
 * One connection, and everything an application does with it.
 * <p>
 * A session belongs to exactly one IO thread for its lifetime, unless a load balancer migrates it. That thread is
 * shared with every other session on the same {@link IOWorker}, which is what makes {@link #processTask(Runnable)}
 * a promise about ordering and a warning about duration at the same time.
 * <p>
 * The instance does not outlive the connection: a reconnect produces a new session, so nothing may be cached
 * across one.
 */
public interface IOSession extends IOWriter {

    /**
     * Adds a tag, which is how {@code Server.broadcast} selects sessions without holding references to them.
     *
     * @param tag the tag to add; adding one twice changes nothing
     */
    void addTag(IOSessionTag tag);

    /**
     * Removes a tag.
     *
     * @param tag the tag to remove; removing one the session does not carry changes nothing
     */
    void removeTag(IOSessionTag tag);

    /**
     * Whether this session carries a tag.
     *
     * @param tag the tag to test
     * @return true if the session carries it
     */
    boolean matchesTag(IOSessionTag tag);

    /**
     * Whether this session carries every one of the given tags.
     *
     * @param tags the tags to test
     * @return true only if all of them are carried
     */
    boolean matchesTags(IOSessionTag... tags);

    /**
     * Whether a thread is this session's IO thread.
     *
     * @param thread the thread to test
     * @return true if it is the thread that runs this session's reads and writes
     */
    boolean isIOThread(Thread thread);

    /**
     * Whether the caller is already on this session's IO thread.
     *
     * @return true when called from it, which is the case inside every {@link IOEventsListener} callback
     */
    boolean isWithinIOThread();

    /**
     * Get the attached object to the session
     *
     * @param <T> the attachment's type, cast unchecked
     * @return the attachment, or null if none was set
     */
    <T> T getAttachment();

    /**
     * Attach an object to the session
     *
     * @param attachment the object to attach, replacing any previous one
     */
    void setAttachment(Object attachment);

    /**
     * The session socket address
     *
     * @return the remote address this session is connected to
     */
    InetSocketAddress getSocketAddress();

    /**
     * The underlying NIO channel this session is bound to.
     *
     * @return the channel; reading or writing it directly bypasses the session and corrupts its state
     */
    NetworkChannel getNetworkChannel();

    /**
     * The session id as defined by {@link BaseBuilder#getId()} or {@link #setId(String)}
     *
     * @return the id, which is what identifies the session in the logs
     */
    String getId();

    /**
     * Update the id of the IO session
     *
     * @param id the new id, typically set once the protocol has told the application who the peer is
     */
    void setId(String id);

    /**
     * Stops the io Session
     *
     * @param stopDeadline time to wait for flushing all message to be sent before closing the session
     */
    void stop(Deadline stopDeadline);

    /**
     * Whether the session is started.
     *
     * @return true between connection and stop
     */
    boolean isStarted();

    /**
     * Runs a task on the session's IO thread, immediately if the caller is already on it.
     * <p>
     * Short tasks only: the thread is shared with every other session bound to the same {@link IOWorker}, so a
     * slow task costs all of them throughput.
     *
     * @param task the task to execute
     */
    void processTask(Runnable task);

    /**
     * Runs a task on the session's IO thread, immediately if the caller is already on it, and reports what
     * happened.
     * <p>
     * Short tasks only: the thread is shared with every other session bound to the same {@link IOWorker}, so a
     * slow task costs all of them throughput.
     *
     * @param task     the task to execute
     * @param callback a callback to be called when the task is processed, successfully or with an error,
     *                 if the session is disconnected and task cannot be processed the implementation should invoke the callback with an EOFException
     */
    void processTask(Runnable task, BiConsumer<Runnable, Exception> callback);

    /**
     * Wait for all messages in the IO worker thread to be sent
     *
     * @param maxWaitTime the maximum wait time
     * @return true if all messages processed within the given deadline
     */
    boolean waitForAllMessagesSent(Deadline maxWaitTime);

    /**
     * Wait for an eventual current read operation to be finished
     *
     * @param maxWaitTime the maximum wait time
     * @return true if read operation have been processed within the given deadline
     */
    boolean waitForAllMessagesRead(Deadline maxWaitTime);

    /**
     * Pauses all current IO read and write operations
     *
     * @param flushAllReadsAndWritesDeadline how long to let the reads and writes already in flight finish before
     *                                       the session stops selecting
     */
    void pause(Deadline flushAllReadsAndWritesDeadline);

    /**
     * Resumes all IO read and write operations
     */
    void resume();

    /**
     * Stops this session collecting statistics, whatever its provider decided at connection time.
     */
    void disableStats();
}
