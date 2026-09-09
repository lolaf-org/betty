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
import org.lolaf.betty.api.Startable;

import java.io.IOException;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * One IO thread and the selector it runs, owning every session registered on it.
 * <p>
 * A worker does all of a session's reads, writes and tasks, so its throughput is shared by all of them - which is
 * what {@link #getLoad()} measures and what an {@link IOWorkerLoadBalancer} acts on.
 */
public interface IOWorker extends Startable<IOWorker> {

    /**
     * Number of session actually bound to this IO worker
     *
     * @return the count, which a migration or a disconnect can change immediately after the call
     */
    int getRegisteredSessionsCount();

    /**
     * Read-only snapshot of the sessions currently registered on this IO worker.
     * Intended for use by {@link IOWorkerLoadBalancer#rebalance(IOWorker[])} implementations.
     *
     * @return the sessions as they were at the call; a rebalance decision made from it may already be stale
     */
    List<IOSession> getRegisteredSessions();

    /**
     * The selector this worker blocks on.
     *
     * @return the selector; registering a channel on it directly bypasses the worker's bookkeeping
     */
    Selector getSelector();

    /**
     * Name of the thread of this IO worker
     *
     * @return the thread name, which is what identifies it in the logs
     */
    String getName();

    /**
     * Name of the thread group of this IO worker
     *
     * @return the owning group's id
     */
    String getGroup();

    /**
     * Adopts an already connected or accepted socket, creating the session for it.
     *
     * @param clientSession true for a socket this side dialled, false for one it accepted; it decides the TLS role
     * @param socket        the connected socket
     * @param baseBuilder   the client or server settings the session is built from
     * @throws IOException if the socket cannot be registered
     */
    void register(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder) throws IOException;


    /**
     * Migrates an IO session from the IOWorker to a target worker
     *
     * @param session the session to migrate
     * @param target  the target new IOWorker
     * @return a CompletableFuture completed once the session is running on its new thread
     */
    CompletableFuture<Void> migrateIOSession(IOSession session, IOWorker target);

    /**
     * The current load of the IO worker, in order to work make sure that a configured {@link IOWorkerLoadBalancer#requiresIOWorkersLoadComputation()} works correctly
     *
     * @return the current IO worker load: the EMA of the total cpu time taken in micros by all IO sessions bound to this IO worker
     */
    double getLoad();

}