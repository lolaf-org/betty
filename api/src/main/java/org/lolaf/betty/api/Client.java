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
import org.lolaf.betty.api.settings.Factory;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.stats.IOStats;
import org.lolaf.betty.api.stats.IOWorkerStats;

/**
 * One outbound connection, opened and then kept open for as long as the client is started.
 * <p>
 * {@link #start()} does not connect and does not fail if nothing is listening: it arms the first attempt, and the
 * addresses in {@link ClientBuilder#getConnectAddresses()} are then tried in turn, cycling until one answers.
 * Connection state is therefore observed through {@link IOEventsListener}, never returned by {@code start}.
 */
public interface Client extends Startable<Client> {

    /**
     * Whether a session is established right now.
     *
     * @return false between connection attempts and after a disconnect, both of which are ordinary states for a
     *         started client
     */
    boolean isConnected();

    /**
     * The builder this client was built from, and so the settings it is running with.
     *
     * @return the builder, never null
     */
    ClientBuilder getBuilder();

    /**
     * The session this client currently holds.
     *
     * @return {@code null} while not connected; a new instance after each reconnect, so it must not be cached
     *         across a disconnect
     */
    IOSession getIOSession();

    /**
     * Toggle switch during runtime to enable or disable {@link IOStats} on current session as configured
     * by {@link ClientBuilder#getIoStatsProvider()}
     *
     * @param enabled true to collect, false to stop collecting; a no-op while disconnected, and the next session
     *                starts from what the provider gives it rather than from this setting
     */
    void enableIOStats(boolean enabled);

    /**
     * Toggle switch during runtime to enable or disable {@link IOWorkerStats} on active IOWorkers as configured
     * by {@link ClientBuilder#getIoWorkersGroup()} {@link IOWorkersGroupSettings#getIoWorkerStatisticsProvider()}
     *
     * @param enabled true to collect, false to stop collecting
     */
    void enableIOWorkerStats(boolean enabled);

    /**
     * Toggle switch during runtime to enable or disable the write buffer pool statistics of the current session.
     *
     * @param enabled true to collect, false to stop collecting; a no-op while disconnected
     */
    void enableIOBufferPoolStats(boolean enabled);

    /**
     * The SPI the implementation module registers, and how {@link ClientBuilder#newInstance()} reaches an
     * implementation without this module depending on one.
     */
    interface ClientFactory extends Factory<Client, ClientBuilder> {

    }
}
