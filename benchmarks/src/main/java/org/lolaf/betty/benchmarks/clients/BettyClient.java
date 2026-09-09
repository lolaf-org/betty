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
package org.lolaf.betty.benchmarks.clients;

import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.ringos.Deadline;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.IdleStrategySelectStrategy;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.ss.WakeupSelectStrategy;
import org.lolaf.betty.api.stats.IOBufferPoolStats;
import org.lolaf.betty.benchmarks.impl.AbstractClientBenchmark;
import org.lolaf.betty.benchmarks.impl.BenchmarkServer;
import org.lolaf.betty.benchmarks.impl.ClientSettings;
import lombok.extern.slf4j.Slf4j;
import net.openhft.affinity.Affinity;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;
import org.lolaf.ringos.threading.FastThreadLocalThread;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

@Slf4j
@State(Scope.Benchmark)
public class BettyClient extends AbstractClientBenchmark {

    private final IOEventsListener ioEventsListener = this::readMessage;
    private Client client;

    private static SelectStrategy getSelectStrategy(ClientSettings clientSettings) {
        return clientSettings.equals(ClientSettings.LOW_LATENCY)
                ? new IdleStrategySelectStrategy(BusySpinIdleStrategy.getInstance()) : new WakeupSelectStrategy(1);
    }

    private boolean readMessage(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
        ByteBuffer outBuffer = session.borrow(message.remaining());
        while (message.remaining() >= BenchmarkServer.PACKET_SIZE) {
            outBuffer.putLong(message.getLong()).putLong(message.getLong());
        }
        session.send(outBuffer, true);
        return message.remaining() == 0;
    }

    @Override
    public boolean isSupported(ClientSettings settings) {
        return true;
    }

    @Override
    public void setupClient(ClientSettings clientSettings) {
        client = ClientBuilder.builder()
                .connectAddress(BenchmarkServer.CONNECT_ADDRESS)
                .ioSettings(IOSettings.builder()
                        .multiThreadedWriteAPICalls(false)
                        .readBufferSize(READ_BUFFER_SIZE)
                        .tasksRingBufferSize(IO_WRITE_BUFFER_POOL_SIZE)
                        .writeIoBufferPoolSettings(IOBufferPoolSettings.builder()
                                .multiThreadingAccessMode(IOBufferPoolSettings.MultiThreadingAccessMode.ONE_BORROWER_THREAD)
                                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                        .poolSize(IO_WRITE_BUFFER_POOL_SIZE)
                                        .bufferSize(IO_WRITE_BUFFER_SIZE)
                                        .build())
                                .ioBufferPoolStatsProvider((s, p) -> new IOBufferPoolStats.LoggingIOBufferPoolStats())
                                .build())
                        .socketOptions(Map.of(StandardSocketOptions.TCP_NODELAY, true))
                        .build())
                .ioWorkersGroup(IOWorkersGroupSettings.builder()
                        .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder()
                                .threadFactory(FastThreadLocalThreadWithAffinity::new)
                                .selectStrategy(getSelectStrategy(clientSettings)).build())
                        .build().newInstance())
                .ioEventsListener(ioEventsListener)
                .build().newInstance().start();
    }

    private static final class FastThreadLocalThreadWithAffinity extends FastThreadLocalThread {

        public FastThreadLocalThreadWithAffinity(Runnable target) {
            super(() -> {
                log.info("Setting IO core affinity to core {}", IO_THREAD_CORE_AFFINITY);
                Affinity.setAffinity(AbstractClientBenchmark.IO_THREAD_CORE_AFFINITY);
                target.run();
            });
        }
    }

    @Override
    public void shutdownClient() {
        client.stop(Deadline.of(Duration.ofSeconds(1)));
    }
}