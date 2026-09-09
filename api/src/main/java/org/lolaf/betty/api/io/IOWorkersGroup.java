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

import org.lolaf.betty.api.Startable;
import org.lolaf.betty.api.settings.Factory;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;

import java.nio.channels.NetworkChannel;
import java.util.List;

/**
 * The IO threads a client or server runs on, and the placement of sessions across them.
 * <p>
 * A group is created from {@link IOWorkersGroupSettings} and can be shared: passing the same group to several
 * clients or servers is how they come to share threads. Which worker a new connection lands on is the
 * {@link IOWorkerLoadBalancer}'s decision, not the group's.
 */
public interface IOWorkersGroup extends Startable<IOWorkersGroup> {

    /**
     * The group's id, which names its threads and so what appears in the logs.
     *
     * @return the id
     */
    String getId();

    /**
     * How many workers, and so how many IO threads, the group runs.
     *
     * @return the worker count, fixed for the group's lifetime
     */
    int getIOWorkersCount();

    /**
     * Picks the worker a channel should be registered on, by asking the configured load balancer.
     *
     * @param networkChannel the channel about to be registered
     * @return the worker that will own it
     */
    IOWorker getNext(NetworkChannel networkChannel);

    /**
     * Turns worker statistics on or off across the whole group at runtime.
     *
     * @param enable true to collect
     */
    void toggleIoWorkerStats(boolean enable);

    /**
     * The SPI the implementation module registers, reached through {@link IOWorkersGroupSettings}.
     */
    interface IOWorkersGroupFactory extends Factory<IOWorkersGroup, IOWorkersGroupSettings> {

    }

    /**
     * The group's workers, in start order.
     *
     * @return the workers; the same order the load balancer is handed them in
     */
    List<IOWorker> getIOWorkers();
}
