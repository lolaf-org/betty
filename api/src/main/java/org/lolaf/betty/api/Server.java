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
package org.lolaf.betty.api;

import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOSessionTag;
import org.lolaf.betty.api.settings.Factory;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.stats.IOStats;
import org.lolaf.betty.api.stats.IOWorkerStats;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Predicate;

/**
 * A listening socket and the sessions accepted on it.
 * <p>
 * Accepted connections are handed to {@link IOEventsListener}; a connection the
 * {@link ServerBuilder#getRemoteSessionsFilter()} turns down never reaches {@code onConnected}. Shutdown has two
 * steps on purpose - {@link #stopListening()} refuses new connections while the established ones keep running, and
 * {@link #stop(org.lolaf.ringos.Deadline)} ends them.
 */
public interface Server extends Startable<Server> {

    /**
     * Stop listening for incoming connections, allows orderly shutdowns
     *
     * @return this server, still started and still serving the sessions it has already accepted
     */
    Server stopListening();

    /**
     * The builder used to create the server
     *
     * @return the builder, and so the settings this server is running with
     */
    ServerBuilder getBuilder();

    /**
     * The list of currently connected sessions
     *
     * @return a snapshot; a session in it may have gone by the time the caller uses it
     */
    List<IOSession> getIOSessions();

    /**
     * Broadcast a message to connected sessions matching a given session tag
     *
     * @param message the message to send, read once and then reusable by the caller
     * @param tag     the tag a session must carry to be sent to
     * @return the number of sessions where the message has been sent
     */
    int broadcast(ByteBuffer message, IOSessionTag tag);

    /**
     * Broadcast a message to connected sessions matching a multiple session tag,
     * each session must match all the tags provided to be selected for message broadcasting
     *
     * @param message the message to send, read once and then reusable by the caller
     * @param tags    the tags a session must all carry to be sent to; every session when null
     * @return the number of sessions where the message has been sent
     */
    int broadcast(ByteBuffer message, IOSessionTag... tags);

    /**
     * Broadcast a message to connected sessions matching a predicate for the IOSession
     *
     * @param message       the message to send, read once and then reusable by the caller
     * @param targetSession decides, per session, whether to send; called on the broadcasting thread
     * @return the number of sessions where the message has been sent
     */
    int broadcast(ByteBuffer message, Predicate<IOSession> targetSession);

    /**
     * Toggle switch during runtime to enable or disable {@link IOStats} on active sessions as configured
     * by {@link ServerBuilder#getIoStatsProvider()}
     *
     * @param enabled true to collect, false to stop collecting; it reaches the sessions connected when it is
     *                called, and a session accepted afterwards starts from what the provider gives it
     */
    void enableIOStats(boolean enabled);

    /**
     * Toggle switch during runtime to enable or disable {@link IOWorkerStats} on active IOWorkers as configured
     * by {@link ServerBuilder#getIoWorkersGroup()} {@link IOWorkersGroupSettings#getIoWorkerStatisticsProvider()}
     *
     * @param enabled true to collect, false to stop collecting
     */
    void enableIOWorkerStats(boolean enabled);

    /**
     * Toggle switch during runtime to enable or disable the write buffer pool statistics of the active sessions.
     *
     * @param enabled true to collect, false to stop collecting; same reach as {@link #enableIOStats(boolean)}
     */
    void enableIOBufferPoolStats(boolean enabled);

    /**
     * The SPI the implementation module registers, and how {@link ServerBuilder#newInstance()} reaches an
     * implementation without this module depending on one.
     */
    interface ServerFactory extends Factory<Server, ServerBuilder> {

    }

}
