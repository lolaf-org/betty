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
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class DedicatedIOWorkerLoadBalancerTest {

    private final DedicatedIOWorkerLoadBalancer lb = DedicatedIOWorkerLoadBalancer.getInstance();

    @Test
    void getInstanceReturnsSingleton() {
        assertThat(DedicatedIOWorkerLoadBalancer.getInstance()).isSameAs(DedicatedIOWorkerLoadBalancer.getInstance());
    }

    @Test
    void doesNotRequireLoadComputation() {
        assertThat(lb.requiresIOWorkersLoadComputation()).isFalse();
    }

    @Test
    void selectIOWorkerPicksAFreeWorker() {
        IOWorker w0 = workerWithCount("w0", 1);
        IOWorker w1 = workerWithCount("w1", 0);
        IOWorker w2 = workerWithCount("w2", 0);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        // first empty worker scanning left to right
        assertThat(selected).isSameAs(w1);
    }

    @Test
    void selectIOWorkerWithAllEmptyWorkersReturnsFirst() {
        IOWorker w0 = workerWithCount("w0", 0);
        IOWorker w1 = workerWithCount("w1", 0);
        IOWorker w2 = workerWithCount("w2", 0);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        assertThat(selected).isSameAs(w0);
    }

    @Test
    void selectIOWorkerWithSingleEmptyWorkerReturnsThatWorker() {
        IOWorker only = workerWithCount("only", 0);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{only});

        assertThat(selected).isSameAs(only);
    }

    @Test
    void selectIOWorkerWhenOversubscribedFallsBackToMinRegistered() {
        IOWorker w0 = workerWithCount("w0", 2);
        IOWorker w1 = workerWithCount("w1", 1);
        IOWorker w2 = workerWithCount("w2", 3);
        IOWorker[] workers = {w0, w1, w2};
        NetworkChannel channel = Mockito.mock(NetworkChannel.class);

        IOWorker selected = lb.selectIOWorker(channel, workers);
        IOWorker minRegisteredPick = MinRegisteredSessionLoadBalancer.getInstance().selectIOWorker(channel, workers);

        assertThat(selected).isSameAs(minRegisteredPick).isSameAs(w1);
    }

    @Test
    void rebalanceReturnsEmptyWhenNoSessions() {
        IOWorker w0 = workerWithSessions("w0");
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenAlreadyDedicated() {
        IOWorker w0 = workerWithSessions("w0", mockSession());
        IOWorker w1 = workerWithSessions("w1", mockSession());
        IOWorker w2 = workerWithSessions("w2", mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1, w2})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenUnderloadedButAlreadySpread() {
        // 1 session, 0, 0 — one worker has its dedicated session, the others are idle. Nothing to do.
        IOWorker w0 = workerWithSessions("w0", mockSession());
        IOWorker w1 = workerWithSessions("w1");
        IOWorker w2 = workerWithSessions("w2");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1, w2})).isEmpty();
    }

    @Test
    void rebalanceSpreadsExtraSessionsOntoEmptyWorkers() {
        IOSession s1 = mockSession();
        IOSession s2 = mockSession();
        // {2, 0, 1} — w0 has one too many, w1 is idle. Expect a single move of one of w0's sessions to w1.
        IOWorker w0 = workerWithSessions("w0", s1, s2);
        IOWorker w1 = workerWithSessions("w1");
        IOWorker w2 = workerWithSessions("w2", mockSession());

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1, w2});

        assertThat(moves).hasSize(1);
        IOWorkerLoadBalancer.SessionMove move = moves.get(0);
        assertThat(move.getTarget()).isSameAs(w1);
        assertThat(move.getSession()).isIn(s1, s2);
    }

    @Test
    void rebalanceWithMultipleEmptyWorkersFillsThemAll() {
        IOSession s1 = mockSession();
        IOSession s2 = mockSession();
        IOSession s3 = mockSession();
        // {3, 0, 0} — w0 has two surplus sessions, w1 and w2 each need one. Expect two moves.
        IOWorker w0 = workerWithSessions("w0", s1, s2, s3);
        IOWorker w1 = workerWithSessions("w1");
        IOWorker w2 = workerWithSessions("w2");

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1, w2});

        assertThat(moves).hasSize(2);
        assertThat(moves).extracting(IOWorkerLoadBalancer.SessionMove::getTarget).containsExactlyInAnyOrder(w1, w2);
        assertThat(moves).allSatisfy(m -> assertThat(m.getSession()).isIn(s1, s2, s3));
    }

    @Test
    void rebalanceWhenOversubscribedFallsBackToMinRegistered() {
        // total = 5 sessions on 2 workers → oversubscribed (5 > 2). Delegate to MinRegistered.
        IOWorker w0 = workerWithSessions("w0", mockSession(), mockSession(), mockSession(), mockSession(), mockSession());
        IOWorker w1 = workerWithSessions("w1");
        IOWorker[] workers = {w0, w1};

        List<IOWorkerLoadBalancer.SessionMove> dedicated = lb.rebalance(workers);
        List<IOWorkerLoadBalancer.SessionMove> minRegistered = MinRegisteredSessionLoadBalancer.getInstance().rebalance(workers);

        // Same move count + same targets in the same order (MinRegistered is deterministic on counts).
        assertThat(dedicated).hasSameSizeAs(minRegistered);
        assertThat(dedicated).extracting(IOWorkerLoadBalancer.SessionMove::getTarget)
                .containsExactlyElementsOf(minRegistered.stream().map(IOWorkerLoadBalancer.SessionMove::getTarget).collect(Collectors.toList()));
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
        List<IOSession> snapshot = new ArrayList<>();
        for (IOSession s : sessions) {
            snapshot.add(s);
        }
        Mockito.when(w.getRegisteredSessions()).thenReturn(snapshot);
        return w;
    }

    private static IOSession mockSession() {
        return Mockito.mock(IOSession.class);
    }
}
