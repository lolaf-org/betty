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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MinIOThreadLoadSessionLoadBalancerTest {

    private final MinIOThreadLoadSessionLoadBalancer lb = MinIOThreadLoadSessionLoadBalancer.getInstance();

    @Test
    void getInstanceReturnsSingleton() {
        assertThat(MinIOThreadLoadSessionLoadBalancer.getInstance()).isSameAs(MinIOThreadLoadSessionLoadBalancer.getInstance());
    }

    @Test
    void requiresLoadComputation() {
        assertThat(lb.requiresIOWorkersLoadComputation()).isTrue();
    }

    @Test
    void selectIOWorkerPicksTheLowestLoadWorker() {
        IOWorker w0 = workerWithLoad("w0", 10.0, 5);
        IOWorker w1 = workerWithLoad("w1", 1.0, 5);
        IOWorker w2 = workerWithLoad("w2", 100.0, 5);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        assertThat(selected).isSameAs(w1);
    }

    @Test
    void selectIOWorkerFallsBackToMinRegisteredSessionWhenAllLoadsAreZero() {
        // The chosen one's load is 0, so the fallback runs and picks the worker with fewest sessions.
        IOWorker w0 = workerWithLoad("w0", 0.0, 9);
        IOWorker w1 = workerWithLoad("w1", 0.0, 1);
        IOWorker w2 = workerWithLoad("w2", 0.0, 7);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        assertThat(selected).isSameAs(w1);
    }

    @Test
    void selectIOWorkerWithSingleWorkerReturnsThatWorkerWhenLoaded() {
        IOWorker only = workerWithLoad("only", 5.0, 3);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{only});

        assertThat(selected).isSameAs(only);
    }

    @Test
    void rebalanceReturnsEmptyWhenSingleWorker() {
        IOWorker only = workerWithLoadAndSessions("only", 5.0, mockSession());

        assertThat(lb.rebalance(new IOWorker[]{only})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenAllLoadsAreZero() {
        IOWorker w0 = workerWithLoadAndSessions("w0", 0.0, mockSession());
        IOWorker w1 = workerWithLoadAndSessions("w1", 0.0, mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenSpreadIsBelowThreshold() {
        // ratio is 0.3 * max → (max - min) must STRICTLY exceed that. 10 vs 8: (10 - 8) = 2 ≤ 3 → no move.
        IOWorker w0 = workerWithLoadAndSessions("w0", 10.0, mockSession());
        IOWorker w1 = workerWithLoadAndSessions("w1", 8.0, mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyAtExactlyThreshold() {
        // 10 vs 7: (10 - 7) = 3, threshold is 0.3 * 10 = 3 → NOT > threshold → no move.
        IOWorker w0 = workerWithLoadAndSessions("w0", 10.0, mockSession());
        IOWorker w1 = workerWithLoadAndSessions("w1", 7.0, mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceEmitsOneMoveWhenSpreadExceedsThreshold() {
        IOSession candidate = mockSession();
        // 10 vs 1: (10 - 1) = 9 > 0.3 * 10 = 3 → emit one move from w0 → w1.
        IOWorker w0 = workerWithLoadAndSessions("w0", 10.0, candidate);
        IOWorker w1 = workerWithLoadAndSessions("w1", 1.0);

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1});

        assertThat(moves).hasSize(1);
        assertThat(moves.get(0).getSession()).isSameAs(candidate);
        assertThat(moves.get(0).getTarget()).isSameAs(w1);
    }

    @Test
    void rebalanceEmitsOnlyOneMovePerPassEvenWithManyImbalances() {
        // Even when multiple workers are over-loaded, the policy is "one move per pass" to let the EMA
        // absorb the change before the next decision.
        IOWorker w0 = workerWithLoadAndSessions("w0", 100.0, mockSession(), mockSession(), mockSession());
        IOWorker w1 = workerWithLoadAndSessions("w1", 90.0, mockSession());
        IOWorker w2 = workerWithLoadAndSessions("w2", 1.0);

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1, w2});

        assertThat(moves).hasSize(1);
        // Most loaded is w0 (100), least is w2 (1). Move from w0 → w2.
        assertThat(moves.get(0).getTarget()).isSameAs(w2);
    }

    @Test
    void rebalanceReturnsEmptyWhenMostLoadedWorkerHasNoSessions() {
        // Defensive: if the most-loaded worker has no registered sessions (e.g. its load is stale),
        // there is nothing to migrate.
        IOWorker w0 = workerWithLoadAndSessions("w0", 10.0);
        IOWorker w1 = workerWithLoadAndSessions("w1", 1.0, mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    private static IOWorker workerWithLoad(String name, double load, int registeredCount) {
        IOWorker w = Mockito.mock(IOWorker.class, name);
        Mockito.when(w.getName()).thenReturn(name);
        Mockito.when(w.getLoad()).thenReturn(load);
        Mockito.when(w.getRegisteredSessionsCount()).thenReturn(registeredCount);
        return w;
    }

    private static IOWorker workerWithLoadAndSessions(String name, double load, IOSession... sessions) {
        IOWorker w = Mockito.mock(IOWorker.class, name);
        Mockito.when(w.getName()).thenReturn(name);
        Mockito.when(w.getLoad()).thenReturn(load);
        Mockito.when(w.getRegisteredSessionsCount()).thenReturn(sessions.length);
        List<IOSession> snapshot = List.of(sessions);
        Mockito.when(w.getRegisteredSessions()).thenReturn(snapshot);
        return w;
    }

    private static IOSession mockSession() {
        return Mockito.mock(IOSession.class);
    }
}
