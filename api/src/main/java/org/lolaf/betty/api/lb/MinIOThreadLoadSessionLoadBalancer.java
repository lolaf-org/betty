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
package org.lolaf.betty.api.lb;

import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkerLoadBalancer;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.channels.NetworkChannel;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Load-aware {@link IOWorkerLoadBalancer} that distributes work by minimizing the per-worker EMA of
 * CPU time spent processing IO operations, as exposed by {@link IOWorker#getLoad()}.
 *
 * <p><b>Placement.</b> {@link #selectIOWorker(NetworkChannel, IOWorker[])} picks the worker with the
 * lowest current load. When all workers report zero load (typical right after startup, before any
 * session is active), it falls back to {@link MinRegisteredSessionLoadBalancer} to avoid pinning every
 * new connection to the same worker.
 *
 * <p><b>Rebalancing.</b> {@link #rebalance(IOWorker[])} finds the most- and least-loaded workers and,
 * if the gap exceeds {@code 0.3 × maxLoad}, emits <b>exactly one</b> {@link SessionMove} per pass.
 * A single move per cycle lets the EMA absorb the migration before the next decision, preventing
 * thrashing under bursty traffic.
 *
 * <p><b>Cost.</b> Because this balancer relies on {@link IOWorker#getLoad()},
 * {@link IOWorkerLoadBalancer#requiresIOWorkersLoadComputation()} returns {@code true}. That forces
 * IO workers to track CPU time per session, which adds {@code System.nanoTime()} calls on the IO hot
 * path. Pick this only when workloads are skewed enough to justify the overhead.
 *
 * <p>Stateless singleton — obtain via {@link #getInstance()}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinIOThreadLoadSessionLoadBalancer implements IOWorkerLoadBalancer {

    private static final MinIOThreadLoadSessionLoadBalancer INSTANCE = new MinIOThreadLoadSessionLoadBalancer();
    /**
     * Move one session per rebalance pass when the load delta between the most-loaded and least-loaded
     * workers exceeds this fraction of the max load. A single move per cycle lets the EMA absorb the
     * effect of the migration before the next decision.
     */
    private static final double REBALANCE_LOAD_DELTA_RATIO = 0.3;

    /**
     * The shared instance; it holds no state, so there is no reason for a second.
     *
     * @return the singleton
     */
    public static MinIOThreadLoadSessionLoadBalancer getInstance() {
        return INSTANCE;
    }

    @Override
    public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
        IOWorker selected = Arrays.stream(ioWorkers).min(Comparator.comparingDouble(IOWorker::getLoad)).orElseThrow();
        if (selected.getLoad() == 0) {
            // we may have only connected session with no activity, in such case fallback to MinRegisteredSessionLoadBalancer
            return MinRegisteredSessionLoadBalancer.getInstance().selectIOWorker(networkChannel, ioWorkers);
        }
        log.info("NetworkChannel {} assigned to IOWorker {} with current load of {}", networkChannel, selected.getName(), selected.getLoad());
        return selected;
    }

    @Override
    public boolean requiresIOWorkersLoadComputation() {
        return true;
    }

    @Override
    public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
        IOWorker mostLoaded = null;
        IOWorker leastLoaded = null;
        double maxLoad = Double.NEGATIVE_INFINITY;
        double minLoad = Double.POSITIVE_INFINITY;
        for (IOWorker w : ioWorkers) {
            double load = w.getLoad();
            if (load > maxLoad) {
                maxLoad = load;
                mostLoaded = w;
            }
            if (load < minLoad) {
                minLoad = load;
                leastLoaded = w;
            }
        }
        if (mostLoaded == null || leastLoaded == null || mostLoaded == leastLoaded || maxLoad <= 0) {
            return List.of();
        }
        if ((maxLoad - minLoad) <= REBALANCE_LOAD_DELTA_RATIO * maxLoad) {
            return List.of();
        }
        IOSession candidate = mostLoaded.getRegisteredSessions().stream().findFirst().orElse(null);
        if (candidate == null) {
            return List.of();
        }
        log.info("Rebalancing one session from {} (load {}) to {} (load {})", mostLoaded.getName(), maxLoad, leastLoaded.getName(), minLoad);
        return List.of(new SessionMove(candidate, leastLoaded));
    }
}
