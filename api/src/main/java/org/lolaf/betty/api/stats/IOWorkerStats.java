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
package org.lolaf.betty.api.stats;

import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.settings.IOSettings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.time.Duration;

/**
 * Per-IO-thread measurements, sampled on a schedule rather than per event.
 * <p>
 * Unlike {@link IOStats}, which is called as the work happens, these methods run once per
 * {@link #getStatsResolution()} between selector passes, so what they report is a period rather than an operation.
 */
public interface IOWorkerStats {

    /**
     * Resolution to call the statistics methods of this listener implementation
     *
     * @return how often a collection runs; shorter costs the IO thread more
     */
    default Duration getStatsResolution() {
        return Duration.ofSeconds(1);
    }

    /**
     * Indicates if the stats are enabled by default or not, if not call {@link Client#enableIOWorkerStats(boolean)} or {@link Server#enableIOWorkerStats(boolean)}
     * to enable/disable it during runtime
     *
     * @return true to start collecting as soon as the worker starts
     */
    default boolean enabledByDefault() {
        return true;
    }

    /**
     * Called when a stats collections is about to start for the given {@link #getStatsResolution()}
     *
     * @param ioWorker  the IOWorker for which the stats are run
     * @param statsTime the current stats time
     */
    default void onStatsCollectionStart(IOWorker ioWorker, long statsTime) {
    }

    /**
     * Called when a stats collections is finished
     *
     * @param ioWorker the IOWorker the collection ran for
     */
    default void onStatsCollectionEnd(IOWorker ioWorker) {
    }

    /**
     * Time this worker spent reading for one session over the period.
     *
     * @param ioWorker            the worker
     * @param ioSession           the session the time was spent on
     * @param cpuTimeTakenInNanos the time, summed over the period
     */
    default void onReadCPUTimeTaken(IOWorker ioWorker, IOSession ioSession, long cpuTimeTakenInNanos) {
    }

    /**
     * Time this worker spent running tasks for one session over the period.
     *
     * @param ioWorker            the worker
     * @param ioSession           the session the time was spent on
     * @param cpuTimeTakenInNanos the time, summed over the period
     */
    default void onTasksCPUTimeTaken(IOWorker ioWorker, IOSession ioSession, long cpuTimeTakenInNanos) {
    }

    /**
     * Time this worker spent writing for one session over the period.
     *
     * @param ioWorker            the worker
     * @param ioSession           the session the time was spent on
     * @param cpuTimeTakenInNanos the time, summed over the period
     */
    default void onWriteCPUTimeTaken(IOWorker ioWorker, IOSession ioSession, long cpuTimeTakenInNanos) {
    }

    /**
     * Time this worker spent completing outbound connections over the period; a client-side cost only.
     *
     * @param ioWorker            the worker
     * @param cpuTimeTakenInNanos the time, summed over the period
     */
    default void onConnectCPUTimeTaken(IOWorker ioWorker, long cpuTimeTakenInNanos) {
    }

    /**
     * Time this worker spent accepting connections over the period; a cost only on the worker hosting the acceptor.
     *
     * @param ioWorker            the worker
     * @param cpuTimeTakenInNanos the time, summed over the period
     */
    default void onAcceptCPUTimeTaken(IOWorker ioWorker, long cpuTimeTakenInNanos) {
    }

    /**
     * Number of selected IO keys since last period
     *
     * @param ioWorker          the worker
     * @param selectedKeysCount how many keys its selector returned over the period; near zero means it is
     *                          spinning on an idle selector
     */
    default void onSelectedKeysCount(IOWorker ioWorker, int selectedKeysCount) {

    }

    /**
     * Indicates the ring buffer for processing IO writes on IO session is full,
     * this indicates that under load or bursts the size of the ring buffer is too small and should maybe
     * be increased, see {@link IOSettings#getTasksRingBufferSize()}
     *
     * @param ioWorker  the IO Worker processing the IO session ring buffer writes
     * @param ioSession the IO session for which the ring buffer is full
     */
    default void onSessionWritesRingBufferFull(IOWorker ioWorker, IOSession ioSession) {

    }

    /**
     * Indicates the ring buffer for processing IO tasks on IO session is full,
     * this indicates that under load or bursts the size of the ring buffer is too small and should maybe
     * be increased, see {@link IOSettings#getTasksRingBufferSize()}
     *
     * @param ioWorker  the IO Worker processing the IO session ring buffer task
     * @param ioSession the IO session for which the ring buffer is full
     */
    default void onSessionTasksRingBufferFull(IOWorker ioWorker, IOSession ioSession) {

    }

    /**
     * Called once per worker to decide what it collects.
     */
    interface IOWorkerStatsProvider {

        /**
         * Decides what a worker about to start will collect.
         *
         * @param ioWorker the worker being started
         * @return the statistics for it; {@link VoidIOWorkerStats#getInstance()} to measure nothing
         */
        IOWorkerStats get(IOWorker ioWorker);

    }

    /**
     * Measures nothing, and is the default.
     */
    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    final class VoidIOWorkerStats implements IOWorkerStats {

        private static final VoidIOWorkerStats INSTANCE = new VoidIOWorkerStats();

        /**
         * The shared instance; it holds no state, so there is no reason for a second.
         *
         * @return the singleton
         */
        public static VoidIOWorkerStats getInstance() {
            return INSTANCE;
        }

        @Override
        public boolean enabledByDefault() {
            return false;
        }
    }
}