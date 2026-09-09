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
import org.lolaf.betty.api.stats.IOWorkerStats;

import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Default {@link IOWorkerLoadBalancer} that distributes work by minimizing the number of registered
 * sessions per {@link IOWorker}.
 *
 * <p><b>Placement.</b> {@link #selectIOWorker(NetworkChannel, IOWorker[])} returns the worker with the
 * smallest {@link IOWorker#getRegisteredSessionsCount()}. Cheap, stateless, and does not require
 * {@link IOWorker#getLoad()} — so it works with the {@link IOWorkerStats.VoidIOWorkerStats}
 * provider and avoids the {@code System.nanoTime()} overhead implied by
 * {@link IOWorkerLoadBalancer#requiresIOWorkersLoadComputation()}.
 *
 * <p><b>Rebalancing.</b> {@link #rebalance(IOWorker[])} computes a target session-count-per-worker
 * (the floor of total / worker-count) and drains workers above {@code target + 1} into workers below
 * the target, producing a single batch of {@link SessionMove}s per call. Each move is later executed
 * by the group as a transparent migration.
 *
 * <p>Use this when sessions have roughly uniform cost; for skewed workloads prefer
 * {@link MinIOThreadLoadSessionLoadBalancer}.
 *
 * <p>Stateless singleton — obtain via {@link #getInstance()}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MinRegisteredSessionLoadBalancer implements IOWorkerLoadBalancer {

    private static final MinRegisteredSessionLoadBalancer INSTANCE = new MinRegisteredSessionLoadBalancer();

    /**
     * The shared instance; it holds no state, so there is no reason for a second.
     *
     * @return the singleton
     */
    public static MinRegisteredSessionLoadBalancer getInstance() {
        return INSTANCE;
    }

    @Override
    public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
        IOWorker selected = Arrays.stream(ioWorkers).min(Comparator.comparingInt(IOWorker::getRegisteredSessionsCount)).orElseThrow();
        log.info("NetworkChannel {} assigned to IOWorker {}", networkChannel, selected.getName());
        return selected;
    }

    @Override
    public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
        int totalRegisteredSessions = 0;
        for (IOWorker w : ioWorkers) {
            totalRegisteredSessions += w.getRegisteredSessionsCount();
        }
        int targetPerWorker = totalRegisteredSessions / ioWorkers.length;
        List<SessionMove> moves = new ArrayList<>();
        int[] counts = new int[ioWorkers.length];
        for (int i = 0; i < ioWorkers.length; i++) {
            counts[i] = ioWorkers[i].getRegisteredSessionsCount();
        }
        for (int from = 0; from < ioWorkers.length; from++) {
            while (counts[from] > targetPerWorker + 1) {
                int to = -1;
                int minCount = Integer.MAX_VALUE;
                for (int j = 0; j < ioWorkers.length; j++) {
                    if (j != from && counts[j] < targetPerWorker && counts[j] < minCount) {
                        minCount = counts[j];
                        to = j;
                    }
                }
                if (to < 0) {
                    break;
                }
                IOSession candidate = ioWorkers[from].getRegisteredSessions().stream().findFirst().orElse(null);
                if (candidate == null) {
                    break;
                }
                moves.add(new SessionMove(candidate, ioWorkers[to]));
                counts[from]--;
                counts[to]++;
            }
        }
        return moves;
    }
}
