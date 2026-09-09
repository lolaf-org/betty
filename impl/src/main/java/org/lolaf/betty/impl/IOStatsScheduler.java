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

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

/**
 * The single daemon thread every statistics collection runs on, shared by all pools and workers in the JVM.
 * <p>
 * One thread rather than one per session is what keeps collection off the IO threads without spending a thread
 * to do it. The first run of a task is aligned to the wall clock rather than to registration time, so tasks
 * sharing a resolution fire on the same tick and their samples cover the same period - which is what makes two
 * sessions' numbers comparable.
 */
@Slf4j
public class IOStatsScheduler {

    private static IOStatsScheduler INSTANCE;
    private final Map<Runnable, ScheduledFuture<?>> tasks;
    private ScheduledExecutorService ioStatsScheduler;

    /**
     * Public only because {@link #getInstance()} has to build one; there is no reason for a second scheduler.
     */
    public IOStatsScheduler() {
        this.ioStatsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "betty-stats-scheduler");
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((t1, e) -> log.error("Uncaught exception occurred in thread {}", t1, e));
            return t;
        });
        this.tasks = new ConcurrentHashMap<>();
    }

    /**
     * The shared scheduler, created on first use.
     *
     * @return the singleton
     */
    public static synchronized IOStatsScheduler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new IOStatsScheduler();
        }
        return INSTANCE;
    }

    private static long calculateInitialStatsDelay(Duration statsResolution) {
        return statsResolution.toMillis() - (System.currentTimeMillis() % statsResolution.toMillis());
    }

    /**
     * Schedules a collection task, aligning its first run to the wall-clock boundary of its resolution.
     *
     * @param statsTask  the task; registering the same instance twice does nothing, so the caller need not track
     *                   whether it already did
     * @param resolution how often it runs
     */
    public void register(Runnable statsTask, Duration resolution) {
        if (tasks.containsKey(statsTask)) {
            return;
        }
        tasks.put(statsTask, ioStatsScheduler.scheduleAtFixedRate(statsTask, calculateInitialStatsDelay(resolution), resolution.toMillis(), TimeUnit.MILLISECONDS));
    }

    /**
     * Cancels a task, interrupting it if it is running.
     *
     * @param statsTask the task to stop; one that was never registered is ignored
     */
    public void unregister(Runnable statsTask) {
        ScheduledFuture<?> scheduledTask = tasks.remove(statsTask);
        if (scheduledTask != null) {
            scheduledTask.cancel(true);
        }
    }
}
