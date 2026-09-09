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
package org.lolaf.betty.api.io;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.channels.NetworkChannel;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class IOWorkerLoadBalancerTest {

    @Test
    void defaultRebalanceReturnsEmptyList() {
        IOWorkerLoadBalancer lb = (channel, workers) -> workers[0];

        assertThat(lb.rebalance(new IOWorker[]{Mockito.mock(IOWorker.class), Mockito.mock(IOWorker.class)})).isEmpty();
    }

    @Test
    void defaultRequiresLoadComputationIsFalse() {
        IOWorkerLoadBalancer lb = (channel, workers) -> workers[0];

        assertThat(lb.requiresIOWorkersLoadComputation()).isFalse();
    }

    @Test
    void selectIOWorkerIsCalledWithGivenArguments() {
        IOWorker w0 = Mockito.mock(IOWorker.class);
        IOWorker w1 = Mockito.mock(IOWorker.class);
        IOWorker[] workers = {w0, w1};
        NetworkChannel channel = Mockito.mock(NetworkChannel.class);
        IOWorkerLoadBalancer lb = (c, ws) -> ws[1];

        assertThat(lb.selectIOWorker(channel, workers)).isSameAs(w1);
    }

    @Test
    void sessionMoveExposesSessionAndTarget() {
        IOSession session = Mockito.mock(IOSession.class);
        IOWorker target = Mockito.mock(IOWorker.class);

        IOWorkerLoadBalancer.SessionMove move = new IOWorkerLoadBalancer.SessionMove(session, target);

        assertThat(move.getSession()).isSameAs(session);
        assertThat(move.getTarget()).isSameAs(target);
    }

    @Test
    void sessionMoveEqualsAndHashCodeAreValueBased() {
        IOSession session = Mockito.mock(IOSession.class);
        IOWorker target = Mockito.mock(IOWorker.class);

        IOWorkerLoadBalancer.SessionMove a = new IOWorkerLoadBalancer.SessionMove(session, target);
        IOWorkerLoadBalancer.SessionMove b = new IOWorkerLoadBalancer.SessionMove(session, target);
        IOWorkerLoadBalancer.SessionMove different = new IOWorkerLoadBalancer.SessionMove(session, Mockito.mock(IOWorker.class));

        assertThat(a).isEqualTo(b)
                .hasSameHashCodeAs(b)
                .isNotEqualTo(different);
    }

    @Test
    void customRebalanceOverrideIsHonored() {
        IOWorker w0 = Mockito.mock(IOWorker.class);
        IOWorker w1 = Mockito.mock(IOWorker.class);
        IOSession session = Mockito.mock(IOSession.class);
        IOWorkerLoadBalancer.SessionMove planned = new IOWorkerLoadBalancer.SessionMove(session, w1);
        IOWorkerLoadBalancer lb = new IOWorkerLoadBalancer() {
            @Override
            public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
                return ioWorkers[0];
            }

            @Override
            public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
                return List.of(planned);
            }
        };

        assertThat(lb.rebalance(new IOWorker[]{w0, w1})).containsExactly(planned);
    }
}
