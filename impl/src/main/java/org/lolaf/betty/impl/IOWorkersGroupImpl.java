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
package org.lolaf.betty.impl;

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkerLoadBalancer;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.threading.FastThreadLocalThread;

import java.nio.channels.NetworkChannel;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
class IOWorkersGroupImpl implements IOWorkersGroup {

    private final IOWorkersGroupSettings ioWorkersGroupSettings;
    private final IOWorkerImpl[] ioWorkers;
    private final AtomicInteger startCalls;
    private final Collection<IOWorkersGroupSettings.IOThreadGroup> ioThreadGroups;
    private ScheduledExecutorService rebalanceScheduler;
    private ScheduledFuture<?> rebalanceTaskHandle;

    IOWorkersGroupImpl(IOWorkersGroupSettings ioWorkersGroupSettings) {
        this.ioWorkersGroupSettings = ioWorkersGroupSettings;
        AtomicInteger ioThreadsCount = new AtomicInteger(0);
        Collection<IOWorkersGroupSettings.IOThreadGroup> ioThreadGroupsLocal = ioWorkersGroupSettings.getIoThreadGroups();
        if (ioThreadGroupsLocal.isEmpty()) {
            ioThreadGroupsLocal = List.of(IOWorkersGroupSettings.IOThreadGroup.builder().build());
        }
        this.ioThreadGroups = ioThreadGroupsLocal;
        this.ioThreadGroups.forEach(g -> ioThreadsCount.addAndGet(g.getIoThreadCount()));
        this.ioWorkers = new IOWorkerImpl[ioThreadsCount.get()];
        this.startCalls = new AtomicInteger();
    }

    @Override
    public String getId() {
        return ioWorkersGroupSettings.getId();
    }

    @Override
    public int getIOWorkersCount() {
        return ioWorkers.length;
    }

    @Override
    public List<IOWorker> getIOWorkers() {
        return Arrays.asList(ioWorkers);
    }

    @Override
    public void toggleIoWorkerStats(boolean enable) {
        for (IOWorkerImpl ioWorker : ioWorkers) {
            ioWorker.toggleIoWorkerStats(enable);
        }
    }

    @Override
    public IOWorkersGroup start() {
        if (startCalls.getAndIncrement() == 0) {
            AtomicInteger i = new AtomicInteger();
            ioThreadGroups.forEach(g -> {
                for (int j = 0; j < g.getIoThreadCount(); j++) {
                    int index = i.getAndIncrement();
                    ioWorkers[index] = new IOWorkerImpl(ioWorkersGroupSettings.getId() + "-" + index, ioWorkersGroupSettings, g).start();
                }
                Class threadClass = ioWorkers[i.get() - 1].getIOWorkerThreadsClass();
                if (!(FastThreadLocalThread.class.isAssignableFrom(threadClass))) {
                    log.warn("IO Threads for group {} created by custom thread factory {} are not FastThreadLocalThread threads. " +
                            "For optimal performances consider setting up a factory providing FastThreadLocalThread threads", g.getName(), threadClass.getName());
                }
            });
            startRebalanceSchedulerIfNeeded();
        }
        return this;
    }

    private void startRebalanceSchedulerIfNeeded() {
        if (ioWorkers.length < 2) {
            return;
        }
        Duration interval = ioWorkersGroupSettings.getIoWorkersRebalanceInterval();
        if (interval == null || interval.isZero() || interval.isNegative()) {
            return;
        }
        rebalanceScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, ioWorkersGroupSettings.getId() + "-io-threads-rebalancer");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((t1, e) -> log.error("Uncaught exception occurred in thread {}", t1, e));
            return t;
        });
        rebalanceTaskHandle = rebalanceScheduler.scheduleAtFixedRate(this::runRebalance, interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void runRebalance() {
        try {
            int total = 0;
            for (IOWorker w : ioWorkers) {
                total += w.getRegisteredSessionsCount();
            }
            if (total == 0) {
                return;
            }
            IOWorkerLoadBalancer lb = ioWorkersGroupSettings.getIoWorkerLoadBalancer();
            List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(ioWorkers);
            if (moves == null || moves.isEmpty()) {
                return;
            }
            for (IOWorkerLoadBalancer.SessionMove move : moves) {
                try {
                    executeMove(move.getSession(), move.getTarget());
                } catch (Exception ex) {
                    log.error("Failed to execute session move {}", move, ex);
                }
            }
        } catch (Throwable t) {
            log.error("Rebalance pass failed for group {}", ioWorkersGroupSettings.getId(), t);
        }
    }

    private void executeMove(IOSession session, IOWorker target) {
        IOWorker source = ((IOSessionImpl) session).getOwnerWorker();
        if (source == null || source == target) {
            return;
        }
        try {
            source.migrateIOSession(session, target).join();
        } catch (CompletionException ex) {
            log.error("Failed to execute IO session IOWorker migration {} to {}", session.getId(), target.getName(), ex);
        }
    }

    @Override
    public IOWorker getNext(NetworkChannel networkChannel) {
        return ioWorkersGroupSettings.getIoWorkerLoadBalancer().selectIOWorker(networkChannel, ioWorkers);
    }

    @Override
    public boolean isStarted() {
        return startCalls.get() > 0;
    }

    /**
     * Stops the workers only for the last connector using this group.
     */
    @Override
    public IOWorkersGroup stop(Deadline stopDeadline) {
        // floored at zero: an extra stop must not push the count negative, which would leave the group unable to ever
        // stop again after the next start
        int usersBeforeStop = startCalls.getAndUpdate(users -> Math.max(0, users - 1));
        if (usersBeforeStop == 0) {
            // never started, or already stopped by its last user - the workers array is not even populated yet
            return this;
        }
        if (usersBeforeStop > 1) {
            log.info("IO workers group '{}' is still used by {} started connector(s), its {} IO threads keep running",
                    ioWorkersGroupSettings.getId(), usersBeforeStop - 1, ioWorkers.length);
            return this;
        }
        if (rebalanceTaskHandle != null) {
            rebalanceTaskHandle.cancel(false);
            rebalanceTaskHandle = null;
        }
        if (rebalanceScheduler != null) {
            rebalanceScheduler.shutdownNow();
            rebalanceScheduler = null;
        }
        Arrays.stream(ioWorkers).forEach(w -> w.stop(stopDeadline));
        return this;
    }

    long getAliveIOWorkersCount() {
        return Arrays.stream(ioWorkers).filter(IOWorkerImpl::isThreadAlive).count();
    }

    public static class IOWorkersGroupFactoryImpl implements IOWorkersGroup.IOWorkersGroupFactory {

        @Override
        public IOWorkersGroup newInstance(IOWorkersGroupSettings settings) {
            return new IOWorkersGroupImpl(settings);
        }
    }
}