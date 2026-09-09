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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Strict 1:1 pinning {@link IOWorkerLoadBalancer} that gives every {@link IOSession} its own
 * dedicated {@link IOWorker} thread.
 *
 * <p><b>Placement.</b> {@link #selectIOWorker(NetworkChannel, IOWorker[])} returns the first worker
 * whose {@link IOWorker#getRegisteredSessionsCount()} is zero — the channel will be the sole
 * session on that thread, eliminating cross-session interference on the IO hot path. Useful when
 * tail latency and predictable per-session CPU are more important than maximizing concurrent
 * connection count.
 *
 * <p><b>Rebalancing.</b> {@link #rebalance(IOWorker[])} restores the 1:1 invariant by moving extra
 * sessions off shared workers onto any empty workers, emitting all required moves in a single
 * batch. Each move is later executed by the group as a transparent migration.
 *
 * <p><b>Oversubscription fallback.</b> When the number of sessions exceeds the number of workers,
 * strict pinning is impossible. In that case both {@link #selectIOWorker} and {@link #rebalance}
 * delegate to {@link MinRegisteredSessionLoadBalancer} to at least keep the load evenly spread,
 * and a rate-limited {@code WARN} is logged so operators can size the worker pool accordingly.
 *
 * <p><b>Cost.</b> Only reads {@link IOWorker#getRegisteredSessionsCount()} and
 * {@link IOWorker#getRegisteredSessions()}; {@link #requiresIOWorkersLoadComputation()} returns
 * {@code false}, so this LB does not force per-session CPU accounting on the IO hot path.
 *
 * <p>Stateless singleton — obtain via {@link #getInstance()}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DedicatedIOWorkerLoadBalancer implements IOWorkerLoadBalancer {

    private static final DedicatedIOWorkerLoadBalancer INSTANCE = new DedicatedIOWorkerLoadBalancer();
    private static final long OVERSUBSCRIPTION_WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(10);

    private volatile long lastOversubscriptionWarnNanos = Long.MIN_VALUE;

    /**
     * The shared instance; the oversubscription warning it rate-limits is shared deliberately, so one warning covers the group.
     *
     * @return the singleton
     */
    public static DedicatedIOWorkerLoadBalancer getInstance() {
        return INSTANCE;
    }

    @Override
    public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
        for (IOWorker w : ioWorkers) {
            if (w.getRegisteredSessionsCount() == 0) {
                log.info("NetworkChannel {} pinned to dedicated IOWorker {}", networkChannel, w.getName());
                return w;
            }
        }
        warnOversubscribed(ioWorkers, "placement");
        return MinRegisteredSessionLoadBalancer.getInstance().selectIOWorker(networkChannel, ioWorkers);
    }

    @Override
    public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
        int total = 0;
        int[] counts = new int[ioWorkers.length];
        for (int i = 0; i < ioWorkers.length; i++) {
            counts[i] = ioWorkers[i].getRegisteredSessionsCount();
            total += counts[i];
        }
        if (total > ioWorkers.length) {
            warnOversubscribed(ioWorkers, "rebalance");
            return MinRegisteredSessionLoadBalancer.getInstance().rebalance(ioWorkers);
        }
        // total <= workers.length: every session can fit on its own worker. Move surplus sessions
        // (any worker with count > 1) onto empty workers (count == 0) until the 1:1 invariant holds.
        List<SessionMove> moves = new ArrayList<>();
        int emptyCursor = 0;
        for (int from = 0; from < ioWorkers.length; from++) {
            while (counts[from] > 1) {
                while (emptyCursor < ioWorkers.length && (emptyCursor == from || counts[emptyCursor] != 0)) {
                    emptyCursor++;
                }
                if (emptyCursor >= ioWorkers.length) {
                    return moves;
                }
                IOSession candidate = ioWorkers[from].getRegisteredSessions().stream().findFirst().orElse(null);
                if (candidate == null) {
                    break;
                }
                moves.add(new SessionMove(candidate, ioWorkers[emptyCursor]));
                counts[from]--;
                counts[emptyCursor]++;
            }
        }
        return moves;
    }

    private void warnOversubscribed(IOWorker[] ioWorkers, String context) {
        long now = System.nanoTime();
        long last = lastOversubscriptionWarnNanos;
        if (last != Long.MIN_VALUE && now - last < OVERSUBSCRIPTION_WARN_INTERVAL_NANOS) {
            return;
        }
        lastOversubscriptionWarnNanos = now;
        int total = 0;
        for (IOWorker w : ioWorkers) {
            total += w.getRegisteredSessionsCount();
        }
        log.warn("DedicatedIOWorkerLoadBalancer oversubscribed during {}: {} sessions for {} IO workers, " +
                        "falling back to MinRegisteredSessionLoadBalancer. Increase the IO worker pool size to restore dedicated pinning.",
                context, total, ioWorkers.length);
    }
}