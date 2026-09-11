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

import org.junit.jupiter.api.Test;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkerLoadBalancer;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.channels.NetworkChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

class NapIdLoadBalancerTest {

    /**
     * The balancer's slot registry is shared by every test in the JVM, so each test uses NAPI IDs nobody
     * else has seen. They start above {@code NR_CPUS} the way the kernel's do.
     */
    private static final AtomicInteger NEXT_NAPID = new AtomicInteger(8193);

    private final NapIdLoadBalancer lb = NapIdLoadBalancer.getInstance();

    private static int freshNapId() {
        return NEXT_NAPID.getAndIncrement();
    }

    private static NetworkChannel channelWithNapId(int napId) throws IOException {
        NetworkChannel channel = Mockito.mock(NetworkChannel.class);
        Mockito.when(channel.getOption(any())).thenReturn(napId);
        return channel;
    }

    private static IOSession sessionWithNapId(int napId) throws IOException {
        NetworkChannel channel = channelWithNapId(napId);
        IOSession session = Mockito.mock(IOSession.class);
        Mockito.when(session.getNetworkChannel()).thenReturn(channel);
        return session;
    }

    private static IOWorker workerWithCount(String name, int count) {
        IOWorker w = Mockito.mock(IOWorker.class, name);
        Mockito.when(w.getName()).thenReturn(name);
        Mockito.when(w.getRegisteredSessionsCount()).thenReturn(count);
        return w;
    }

    private static IOWorker workerWithSessions(String name, IOSession... sessions) {
        IOWorker w = Mockito.mock(IOWorker.class, name);
        Mockito.when(w.getName()).thenReturn(name);
        Mockito.when(w.getRegisteredSessionsCount()).thenReturn(sessions.length);
        List<IOSession> snapshot = List.of(sessions);
        Mockito.when(w.getRegisteredSessions()).thenReturn(snapshot);
        return w;
    }

    private static void hostSessions(IOWorker worker, IOSession... sessions) {
        List<IOSession> snapshot = List.of(sessions);
        Mockito.when(worker.getRegisteredSessionsCount()).thenReturn(sessions.length);
        Mockito.when(worker.getRegisteredSessions()).thenReturn(snapshot);
    }

    @Test
    void getInstanceReturnsSingleton() {
        assertThat(NapIdLoadBalancer.getInstance()).isSameAs(NapIdLoadBalancer.getInstance());
    }

    @Test
    void doesNotRequireLoadComputation() {
        assertThat(lb.requiresIOWorkersLoadComputation()).isFalse();
    }

    @Test
    void selectIOWorkerKeepsOneNapiIdOnOneWorker() throws IOException {
        IOWorker[] workers = {workerWithCount("w0", 5), workerWithCount("w1", 5), workerWithCount("w2", 5)};
        int napId = freshNapId();

        IOWorker first = lb.selectIOWorker(channelWithNapId(napId), workers);
        IOWorker second = lb.selectIOWorker(channelWithNapId(napId), workers);

        assertThat(second).isSameAs(first);
    }

    @Test
    void selectIOWorkerSpreadsDistinctNapiIdsOverWorkers() throws IOException {
        IOWorker[] workers = {workerWithCount("w0", 5), workerWithCount("w1", 5), workerWithCount("w2", 5)};

        IOWorker a = lb.selectIOWorker(channelWithNapId(freshNapId()), workers);
        IOWorker b = lb.selectIOWorker(channelWithNapId(freshNapId()), workers);
        IOWorker c = lb.selectIOWorker(channelWithNapId(freshNapId()), workers);

        assertThat(List.of(a, b, c)).containsExactlyInAnyOrder(workers[0], workers[1], workers[2]);
    }

    @Test
    void selectIOWorkerAcceptsKernelSizedNapiIds() throws IOException {
        // A real NAPI ID is far larger than the worker count; it must still place by ID rather than fall
        // back, which the changing session counts here would betray.
        IOWorker w0 = workerWithCount("w0", 1);
        IOWorker w1 = workerWithCount("w1", 9);
        IOWorker[] workers = {w0, w1};
        int napId = freshNapId();

        IOWorker first = lb.selectIOWorker(channelWithNapId(napId), workers);
        Mockito.when(w0.getRegisteredSessionsCount()).thenReturn(9);
        Mockito.when(w1.getRegisteredSessionsCount()).thenReturn(1);
        IOWorker second = lb.selectIOWorker(channelWithNapId(napId), workers);

        assertThat(second).isSameAs(first);
    }

    @Test
    void selectIOWorkerFallsBackWhenNapiIdIsZero() throws IOException {
        // napId == 0 means the kernel has not yet associated the socket with an RX queue
        // → fallback to MinRegisteredSession.
        IOWorker w0 = workerWithCount("w0", 9);
        IOWorker w1 = workerWithCount("w1", 1);
        NetworkChannel channel = channelWithNapId(0);

        IOWorker selected = lb.selectIOWorker(channel, new IOWorker[]{w0, w1});

        assertThat(selected).isSameAs(w1);
    }

    @Test
    void selectIOWorkerFallsBackWhenGetOptionThrows() throws IOException {
        IOWorker w0 = workerWithCount("w0", 1);
        IOWorker w1 = workerWithCount("w1", 9);
        NetworkChannel channel = Mockito.mock(NetworkChannel.class);
        Mockito.when(channel.getOption(any())).thenThrow(new IOException("testing exception"));

        IOWorker selected = lb.selectIOWorker(channel, new IOWorker[]{w0, w1});

        assertThat(selected).isSameAs(w0);
    }

    @Test
    void rebalanceEmitsMoveWhenNapiIdDisagreesWithCurrentPlacement() throws IOException {
        IOWorker w0 = workerWithSessions("w0");
        IOWorker w1 = workerWithSessions("w1");
        IOWorker[] workers = {w0, w1};
        int napId = freshNapId();
        IOWorker target = lb.selectIOWorker(channelWithNapId(napId), workers);
        IOWorker other = target == w0 ? w1 : w0;
        IOSession misplaced = sessionWithNapId(napId);
        hostSessions(other, misplaced);

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(workers);

        assertThat(moves).hasSize(1);
        assertThat(moves.get(0).getSession()).isSameAs(misplaced);
        assertThat(moves.get(0).getTarget()).isSameAs(target);
    }

    @Test
    void rebalanceEmitsNoMoveWhenNapiIdMatchesCurrentPlacement() throws IOException {
        IOWorker w0 = workerWithSessions("w0");
        IOWorker w1 = workerWithSessions("w1");
        IOWorker[] workers = {w0, w1};
        int napId = freshNapId();
        IOWorker target = lb.selectIOWorker(channelWithNapId(napId), workers);
        hostSessions(target, sessionWithNapId(napId));

        assertThat(lb.rebalance(workers)).isEmpty();
    }

    @Test
    void rebalanceSkipsSessionsWithNapiIdZero() throws IOException {
        IOSession unassigned = sessionWithNapId(0);
        IOWorker w0 = workerWithSessions("w0", unassigned);
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceSkipsSessionsWithNullNetworkChannel() {
        IOSession session = Mockito.mock(IOSession.class);
        Mockito.when(session.getNetworkChannel()).thenReturn(null);
        IOWorker w0 = workerWithSessions("w0", session);
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceSkipsSessionWhenGetOptionThrows() throws IOException {
        NetworkChannel throwing = Mockito.mock(NetworkChannel.class);
        Mockito.when(throwing.getOption(any())).thenThrow(new IOException("boom"));
        IOSession session = Mockito.mock(IOSession.class);
        Mockito.when(session.getNetworkChannel()).thenReturn(throwing);
        IOWorker w0 = workerWithSessions("w0", session);
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceEmitsMovesForMultipleMisplacedSessions() throws IOException {
        IOWorker w0 = workerWithSessions("w0");
        IOWorker w1 = workerWithSessions("w1");
        IOWorker[] workers = {w0, w1};
        int firstNapId = freshNapId();
        int secondNapId = freshNapId();
        IOWorker firstTarget = lb.selectIOWorker(channelWithNapId(firstNapId), workers);
        IOWorker secondTarget = lb.selectIOWorker(channelWithNapId(secondNapId), workers);
        // consecutive slots land on consecutive workers, so two fresh IDs never share one
        assertThat(secondTarget).isNotSameAs(firstTarget);
        IOSession first = sessionWithNapId(firstNapId);
        IOSession second = sessionWithNapId(secondNapId);
        hostSessions(firstTarget, second);
        hostSessions(secondTarget, first);

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(workers);

        assertThat(moves).hasSize(2)
                .anySatisfy(m -> {
                    assertThat(m.getSession()).isSameAs(first);
                    assertThat(m.getTarget()).isSameAs(firstTarget);
                }).anySatisfy(m -> {
                    assertThat(m.getSession()).isSameAs(second);
                    assertThat(m.getTarget()).isSameAs(secondTarget);
                });
    }
}
