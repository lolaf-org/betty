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
import org.lolaf.betty.api.io.IOBufferPool;
import org.lolaf.betty.api.io.IOSession;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;

/**
 * Measurements of a session's write buffer pool, sampled once per {@link #getStatsResolution()}.
 * <p>
 * What they are for is sizing: a pool too small still works, falling back to allocation, so the only sign of it is
 * a miss count here. {@link LoggingIOBufferPoolStats} is enough to notice that without writing an implementation.
 */
public interface IOBufferPoolStats {

    /**
     * Resolution to call the statistics methods of this listener implementation
     *
     * @return how often a collection runs
     */
    default Duration getStatsResolution() {
        return Duration.ofSeconds(1);
    }

    /**
     * Indicates if the stats are enabled by default or not, if not call {@link Client#enableIOBufferPoolStats(boolean)} or {@link Server#enableIOBufferPoolStats(boolean)}
     * to enable/disable it during runtime
     *
     * @return true to start collecting as soon as the pool is bound to a session
     */
    default boolean enabledByDefault() {
        return true;
    }

    /**
     * Called when a stats collections is about to start for the given {@link #getStatsResolution()}
     *
     * @param ioSession the IOSession for which the {@link IOBufferPool} is bound
     * @param statsTime the current stats time
     */
    default void startStatsCollection(IOSession ioSession, long statsTime) {
    }

    /**
     * Called when a stats collections is finished
     *
     * @param ioSession the session whose pool the collection ran for
     */
    default void endStatsCollection(IOSession ioSession) {
    }

    /**
     * Buffers asked for that were larger than any zone could serve, so they were allocated instead. A persistent
     * count here means a zone is missing, not that a zone is too small.
     *
     * @param ioSession the session
     * @param missCount how many over the period
     */
    default void onOversizeBufferMiss(IOSession ioSession, int missCount) {
    }

    /**
     * Buffers a zone could have served but had none left of, so they were allocated instead. This is the count
     * that says a zone should hold more buffers.
     *
     * @param ioSession      the session
     * @param bufferSizeZone the zone, by its configured buffer size
     * @param missCount      how many over the period
     */
    default void onStarvedPoolBufferMiss(IOSession ioSession, int bufferSizeZone, int missCount) {
    }

    /**
     * How much of a zone is idle, reported per zone so that one starving beside another sitting full is visible.
     *
     * @param ioSession        the session
     * @param bufferSizeZone   the zone, by its configured buffer size
     * @param buffersCount     how many buffers the zone holds in all
     * @param freeBuffersCount how many of them were available at the sample
     */
    default void onFreeBufferCount(IOSession ioSession, int bufferSizeZone, int buffersCount, int freeBuffersCount) {
    }

    /**
     * Borrows a zone served from its own buffers.
     *
     * @param ioSession      the session
     * @param bufferSizeZone the zone, by its configured buffer size
     * @param hitCounts      how many over the period
     */
    default void onPooledBufferHits(IOSession ioSession, int bufferSizeZone, int hitCounts) {
    }

    /**
     * The pool's hit ratio over the period, across every zone.
     *
     * @param ioSession     the session
     * @param totalRequests how many buffers were asked for
     * @param hitsRatio     the share served from the pool, 0 to 1
     */
    default void onHitRatio(IOSession ioSession, int totalRequests, double hitsRatio) {
    }

    /**
     * Called once per pool to decide what it collects.
     */
    interface IOBufferPoolStatsProvider {

        /**
         * Decides what a pool about to be bound will collect.
         *
         * @param ioSession    the session the pool is being bound to
         * @param ioBufferPool the pool itself, so an implementation can read {@link IOBufferPool#getFreeBufferCount()}
         * @return the statistics for it, or null to measure nothing
         */
        IOBufferPoolStats get(IOSession ioSession, IOBufferPool ioBufferPool);

    }

    /**
     * Logs only what wants acting on - misses, and a hit ratio under target - so an idle pool says nothing.
     */
    @Slf4j
    class LoggingIOBufferPoolStats implements IOBufferPoolStats {

        private final double targetHitsRatio;

        /**
         * Logs a hit ratio below a chosen target.
         *
         * @param targetHitsRatio the ratio below which the hit ratio is logged, 0 to 1
         */
        public LoggingIOBufferPoolStats(double targetHitsRatio) {
            this.targetHitsRatio = targetHitsRatio;
        }

        /**
         * Logs a hit ratio below 0.9, a pool missing a tenth of its borrows being worth sizing rather than
         * ignoring.
         */
        public LoggingIOBufferPoolStats() {
            this.targetHitsRatio = 0.9;
        }

        @Override
        public void onOversizeBufferMiss(IOSession ioSession, int missCount) {
            if (missCount > 0) {
                log.info("Oversized IOBufferPool misses on IOSession {}: {}", ioSession.getId(), missCount);
            }
        }

        @Override
        public void onStarvedPoolBufferMiss(IOSession ioSession, int bufferSizeZone, int missCount) {
            if (missCount > 0) {
                log.info("Starved IOBufferPool on zone {} for IOSession {}: {} misses", bufferSizeZone, ioSession.getId(), missCount);
            }
        }

        @Override
        public void onHitRatio(IOSession ioSession, int totalRequests, double hitsRatio) {
            if (hitsRatio < targetHitsRatio) {
                log.info("IOBufferPool hits ratio under {}% on IOSession {}: {}", targetHitsRatio * 100, ioSession.getId(), hitsRatio * 100);
            }
        }
    }
}