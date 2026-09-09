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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;

class NapIdLoadBalancerTest {

    private final NapIdLoadBalancer lb = NapIdLoadBalancer.getInstance();

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

    @Test
    void getInstanceReturnsSingleton() {
        assertThat(NapIdLoadBalancer.getInstance()).isSameAs(NapIdLoadBalancer.getInstance());
    }

    @Test
    void doesNotRequireLoadComputation() {
        assertThat(lb.requiresIOWorkersLoadComputation()).isFalse();
    }

    @Test
    void selectIOWorkerMapsNapiIdToZeroIndexedWorker() throws IOException {
        IOWorker w0 = workerWithCount("w0", 5);
        IOWorker w1 = workerWithCount("w1", 5);
        IOWorker w2 = workerWithCount("w2", 5);
        NetworkChannel channel = channelWithNapId(2);

        IOWorker selected = lb.selectIOWorker(channel, new IOWorker[]{w0, w1, w2});

        // napId 2 → workers[1]
        assertThat(selected).isSameAs(w1);
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
    void selectIOWorkerFallsBackWhenNapiIdIsOutOfRange() throws IOException {
        IOWorker w0 = workerWithCount("w0", 1);
        IOWorker w1 = workerWithCount("w1", 9);
        // Only 2 workers but napId 5 → out of range → fallback to MinRegisteredSession.
        NetworkChannel channel = channelWithNapId(5);

        IOWorker selected = lb.selectIOWorker(channel, new IOWorker[]{w0, w1});

        assertThat(selected).isSameAs(w0);
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
        IOSession misplaced = sessionWithNapId(2);          // wants workers[1]
        IOWorker w0 = workerWithSessions("w0", misplaced);  // currently on workers[0]
        IOWorker w1 = workerWithSessions("w1");

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1});

        assertThat(moves).hasSize(1);
        assertThat(moves.get(0).getSession()).isSameAs(misplaced);
        assertThat(moves.get(0).getTarget()).isSameAs(w1);
    }

    @Test
    void rebalanceEmitsNoMoveWhenNapiIdMatchesCurrentPlacement() throws IOException {
        IOSession wellPlaced = sessionWithNapId(1);          // wants workers[0]
        IOWorker w0 = workerWithSessions("w0", wellPlaced);
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
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
        IOSession s0to1 = sessionWithNapId(2);  // wants workers[1]
        IOSession s1to0 = sessionWithNapId(1);  // wants workers[0]
        IOWorker w0 = workerWithSessions("w0", s0to1);
        IOWorker w1 = workerWithSessions("w1", s1to0);

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1});

        assertThat(moves).hasSize(2)
                .anySatisfy(m -> {
                    assertThat(m.getSession()).isSameAs(s0to1);
                    assertThat(m.getTarget()).isSameAs(w1);
                }).anySatisfy(m -> {
                    assertThat(m.getSession()).isSameAs(s1to0);
                    assertThat(m.getTarget()).isSameAs(w0);
                });
    }
}
