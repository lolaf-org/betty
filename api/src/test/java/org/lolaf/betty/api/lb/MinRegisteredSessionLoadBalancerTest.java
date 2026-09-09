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

import static org.assertj.core.api.Assertions.assertThat;

class MinRegisteredSessionLoadBalancerTest {

    private final MinRegisteredSessionLoadBalancer lb = MinRegisteredSessionLoadBalancer.getInstance();

    @Test
    void getInstanceReturnsSingleton() {
        assertThat(MinRegisteredSessionLoadBalancer.getInstance()).isSameAs(MinRegisteredSessionLoadBalancer.getInstance());
    }

    @Test
    void doesNotRequireLoadComputation() {
        assertThat(lb.requiresIOWorkersLoadComputation()).isFalse();
    }

    @Test
    void selectIOWorkerPicksTheWorkerWithFewestSessions() {
        IOWorker w0 = workerWithCount("w0", 5);
        IOWorker w1 = workerWithCount("w1", 2);
        IOWorker w2 = workerWithCount("w2", 7);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        assertThat(selected).isSameAs(w1);
    }

    @Test
    void selectIOWorkerWithSingleWorkerReturnsThatWorker() {
        IOWorker only = workerWithCount("only", 42);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{only});

        assertThat(selected).isSameAs(only);
    }

    @Test
    void selectIOWorkerWithTieReturnsOneOfTheTiedWorkers() {
        IOWorker w0 = workerWithCount("w0", 3);
        IOWorker w1 = workerWithCount("w1", 3);
        IOWorker w2 = workerWithCount("w2", 3);

        IOWorker selected = lb.selectIOWorker(Mockito.mock(NetworkChannel.class), new IOWorker[]{w0, w1, w2});

        assertThat(selected).isIn(w0, w1, w2);
    }

    @Test
    void rebalanceReturnsEmptyWhenNoSessions() {
        IOWorker w0 = workerWithSessions("w0");
        IOWorker w1 = workerWithSessions("w1");

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenWorkersAreAlreadyBalanced() {
        IOWorker w0 = workerWithSessions("w0", mockSession(), mockSession());
        IOWorker w1 = workerWithSessions("w1", mockSession(), mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceReturnsEmptyWhenSpreadIsAtMostOne() {
        // 3 sessions on w0, 2 on w1 — target = floor(5/2) = 2, w0 has target+1 not target+2: no move.
        IOWorker w0 = workerWithSessions("w0", mockSession(), mockSession(), mockSession());
        IOWorker w1 = workerWithSessions("w1", mockSession(), mockSession());

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).isEmpty();
    }

    @Test
    void rebalanceMovesSessionFromOverloadedToUnderloadedWorker() {
        IOSession s1 = mockSession();
        IOSession s2 = mockSession();
        IOSession s3 = mockSession();
        IOSession s4 = mockSession();
        IOSession s5 = mockSession();
        // 5 sessions on w0, 0 on w1 — target = floor(5/2) = 2, stops once w0 ≤ target+1.
        IOWorker w0 = workerWithSessions("w0", s1, s2, s3, s4, s5);
        IOWorker w1 = workerWithSessions("w1");

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1});

        // After: w0 = 3 = target + 1 (loop exit condition), w1 = 2 = target. One move.
        assertThat(moves).hasSize(2)
                .allSatisfy(m -> assertThat(m.getTarget()).isSameAs(w1))
                .allSatisfy(m -> assertThat(m.getSession()).isIn(s1, s2, s3, s4, s5));
    }

    @Test
    void rebalanceDistributesAcrossMultipleUnderloadedWorkers() {
        IOSession s1 = mockSession();
        IOSession s2 = mockSession();
        IOSession s3 = mockSession();
        IOSession s4 = mockSession();
        // 4 sessions on w0, 0 on w1, 0 on w2 — target = floor(4/3) = 1, w0 has target+3: emit 2 moves.
        IOWorker w0 = workerWithSessions("w0", s1, s2, s3, s4);
        IOWorker w1 = workerWithSessions("w1");
        IOWorker w2 = workerWithSessions("w2");

        List<IOWorkerLoadBalancer.SessionMove> moves = lb.rebalance(new IOWorker[]{w0, w1, w2});

        assertThat(moves).hasSize(2);
        assertThat(moves).extracting(IOWorkerLoadBalancer.SessionMove::getTarget).containsExactlyInAnyOrder(w1, w2);
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
