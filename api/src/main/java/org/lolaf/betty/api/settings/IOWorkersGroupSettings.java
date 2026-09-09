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
package org.lolaf.betty.api.settings;

import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkerLoadBalancer;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.lb.MinRegisteredSessionLoadBalancer;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.ss.WakeupSelectStrategy;
import org.lolaf.betty.api.stats.IOWorkerStats;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import org.lolaf.ringos.threading.FastThreadLocalThread;

import java.nio.channels.spi.SelectorProvider;
import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ThreadFactory;

/**
 * The IO threads a client or server runs on. Building one group and passing it to several clients or servers is
 * how they come to share threads.
 */
@Getter
@Builder(toBuilder = true)
public class IOWorkersGroupSettings implements InstanceProvider<IOWorkersGroup> {

    /**
     * Names the group's threads, and so what identifies them in the logs.
     */
    @Builder.Default
    private final String id = "default";
    /**
     * IO worker load balancer to assign a new connection to an IO worker thread in the group
     */
    @Builder.Default
    private final IOWorkerLoadBalancer ioWorkerLoadBalancer = MinRegisteredSessionLoadBalancer.getInstance();
    /**
     * The thread groups making up this group. Several exist so that threads can differ in select strategy - one
     * busy-spinning for latency, another sleeping for the rest - within a single group.
     */
    @Singular
    private final Collection<IOThreadGroup> ioThreadGroups;
    /**
     * Java NIO selector provider, this normally does not need ot be changed
     */
    @Builder.Default
    private final SelectorProvider selectorProvider = SelectorProvider.provider();
    /**
     * Enable memory allocation an CPU usage optimizations on NIO selector, should be disabled only if JDK crashes on startup due to this optimization
     */
    @Builder.Default
    private final boolean optimizedSelector = true;

    /**
     * Time window for the {@link IOWorker#getLoad()} EMA calculation
     */
    @Builder.Default
    private final Duration workersLoadEMATimeWindow = Duration.ofMinutes(5);

    /**
     * IO worker statistics provider to calculate IO session stats such as CPU time take to read/write/process tasks in the IO threads.
     * Providing a IOWorkerStats bring a small performance hit as the stats needs to be tracked by the IO Sessions
     */
    @Builder.Default
    private final IOWorkerStats.IOWorkerStatsProvider ioWorkerStatisticsProvider = ioWorker -> IOWorkerStats.VoidIOWorkerStats.getInstance();

    /**
     * Interval at which the configured {@link IOWorkerLoadBalancer#rebalance(IOWorker[])} is invoked to
     * redistribute existing sessions across IO workers (transparent migration: cancel + re-register the
     * channel on the target worker's selector). When {@code null} (the default), no rebalancing scheduler
     * is created and the placement decision at connect time is final.
     */
    private final Duration ioWorkersRebalanceInterval;

    @Override
    public IOWorkersGroup newInstance() {
        return InstanceProvider.getSpiInstance(this, IOWorkersGroup.IOWorkersGroupFactory.class);
    }

    /**
     * A set of IO threads sharing one select strategy and one thread factory.
     */
    @Getter
    @Builder(toBuilder = true)
    public static class IOThreadGroup {

        /**
         * Thread factory for the IO thread, if you provide one please try to return thread factory extending a {@link FastThreadLocalThread} to improve perfs
         */
        @Builder.Default
        private final ThreadFactory threadFactory = FastThreadLocalThread::new;

        /**
         * NIO Selector select strategy for the IO worker threads in the group, important for latency and throughput tunings
         */
        @Builder.Default
        private final SelectStrategy selectStrategy = new WakeupSelectStrategy(10);

        /**
         * Name of the thread group
         */
        @Builder.Default
        private final String name = "default";

        /**
         * Number of IO threads in the thread group
         */
        @Builder.Default
        private final int ioThreadCount = 1;
    }
}