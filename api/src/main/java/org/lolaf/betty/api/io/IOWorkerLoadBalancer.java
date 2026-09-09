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

import lombok.Value;
import org.lolaf.betty.api.lb.MinIOThreadLoadSessionLoadBalancer;
import org.lolaf.betty.api.lb.MinRegisteredSessionLoadBalancer;
import org.lolaf.betty.api.lb.NapIdLoadBalancer;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;

import java.nio.channels.NetworkChannel;
import java.util.List;

/**
 * Strategy for assigning IO sessions to {@link IOWorker} threads within an
 * {@link IOWorkersGroup}. Drives two distinct decisions:
 * <ul>
 *     <li>{@link #selectIOWorker(NetworkChannel, IOWorker[])} — initial placement at connect/accept time,
 *         called once per new {@link NetworkChannel}.</li>
 *     <li>{@link #rebalance(IOWorker[])} — periodic re-placement of already-registered sessions, invoked
 *         on the schedule set via
 *         {@link IOWorkersGroupSettings#getIoWorkersRebalanceInterval()}.
 *         The group executes each returned {@link SessionMove} as a transparent migration (cancel the
 *         {@link java.nio.channels.SelectionKey} on the source worker, re-register the channel on the
 *         target worker's selector) without disconnecting the session.</li>
 * </ul>
 *
 * <p>Implementations are typically stateless singletons; the built-ins under {@code org.lolaf.betty.api.lb}
 * cover the common cases:
 * <ul>
 *     <li>{@link MinRegisteredSessionLoadBalancer} — packs by registered-session count.</li>
 *     <li>{@link MinIOThreadLoadSessionLoadBalancer} — packs by EMA-tracked CPU load.</li>
 *     <li>{@link NapIdLoadBalancer} — pins sessions to the IO worker matching the NIC RX
 *         queue reported by {@code SO_INCOMING_NAPI_ID}.</li>
 *     <li>{@link org.lolaf.betty.api.lb.DedicatedIOWorkerLoadBalancer} — one session per worker, falling back to
 *         {@link MinRegisteredSessionLoadBalancer} once there are more sessions than workers.</li>
 * </ul>
 */
public interface IOWorkerLoadBalancer {

    /**
     * Pick the {@link IOWorker} that should own a newly connected or accepted channel. Called exactly
     * once per channel, before it is registered on any selector.
     *
     * @param networkChannel the freshly created channel (typically a {@link java.nio.channels.SocketChannel}
     *                       on connect/accept, or a {@link java.nio.channels.ServerSocketChannel} when the
     *                       group hosts the acceptor)
     * @param ioWorkers      all workers in the group, in start order; never empty
     * @return the worker to assign the channel to; must be one of {@code ioWorkers}
     */
    IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers);

    /**
     * Indicates that the load balancer implementation requires calculation of {@link IOWorker#getLoad()}.
     * This brings a small performance hit as it required more calls to System.nanotime() to calculate IOWorker consumed CPU time
     *
     * @return true to have the group measure worker load; false, the default, leaves it uncomputed
     */
    default boolean requiresIOWorkersLoadComputation() {
        return false;
    }

    /**
     * Periodically invoked when {@link IOWorkersGroupSettings#getIoWorkersRebalanceInterval()} is configured.
     * Returns the list of session migrations to perform; the group executes each move via transparent migration
     * (cancel the SelectionKey on the source worker and re-register the channel on the target worker's selector).
     * The default is a no-op.
     *
     * @param ioWorkers all workers in the group, in start order
     * @return the moves to perform, empty for none
     */
    default List<SessionMove> rebalance(IOWorker[] ioWorkers) {
        return List.of();
    }

    /**
     * One session to move, and where to.
     */
    @Value
    class SessionMove {
        /**
         * The session to migrate.
         */
        IOSession session;
        /**
         * The worker it should end up on.
         */
        IOWorker target;
    }
}