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
import jdk.net.ExtendedSocketOptions;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.SocketOption;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * NIC-aware {@link IOWorkerLoadBalancer} that pins each session to the {@link IOWorker} matching the
 * kernel's NAPI ID (i.e. the NIC RX queue) reported by {@code ExtendedSocketOptions.SO_INCOMING_NAPI_ID}.
 * The intent is to keep each session's IO thread on the same CPU/queue as the kernel softirq processing
 * its inbound packets, reducing cross-core wakeups and cache traffic.
 *
 * <p><b>Placement.</b> {@link #selectIOWorker(NetworkChannel, IOWorker[])} reads
 * {@code SO_INCOMING_NAPI_ID} from the channel and maps queue id {@code N} to {@code ioWorkers[N - 1]}.
 * The NAPI ID is often {@code 0} at connect time (the kernel has not yet associated the socket with a
 * queue), in which case this balancer falls back to {@link MinRegisteredSessionLoadBalancer} for the
 * initial placement and the periodic {@link #rebalance(IOWorker[])} re-evaluates it later.
 *
 * <p><b>Rebalancing.</b> {@link #rebalance(IOWorker[])} walks every registered session, reads its
 * current NAPI ID, and emits a {@link SessionMove} whenever the implied worker differs from the
 * current owner — fixing up initial placements that landed on the fallback worker and following any
 * NIC-side rebalancing of RX queues over the session's lifetime.
 *
 * <p><b>Requirements.</b> Needs JDK 15+ for {@code ExtendedSocketOptions.SO_INCOMING_NAPI_ID}, a Linux
 * kernel that exposes NAPI IDs, and a network setup where the number of IO workers matches the number
 * of NIC RX queues (so {@code napId - 1} indexes a valid worker).
 *
 * <p>Stateless singleton — obtain via {@link #getInstance()}.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NapIdLoadBalancer implements IOWorkerLoadBalancer {

    private static final SocketOption<Integer> NAPID_SO = getNapIdSo();
    private static final MinRegisteredSessionLoadBalancer FALLBACK = MinRegisteredSessionLoadBalancer.getInstance();
    private static final NapIdLoadBalancer INSTANCE = new NapIdLoadBalancer();

    /**
     * The shared instance; it holds no state, so there is no reason for a second.
     *
     * @return the singleton
     */
    public static NapIdLoadBalancer getInstance() {
        return INSTANCE;
    }

    private static SocketOption<Integer> getNapIdSo() {
        Field[] soFields = ExtendedSocketOptions.class.getDeclaredFields();
        Field napiIdField = Stream.of(soFields).filter(f -> f.getName().equals("SO_INCOMING_NAPI_ID"))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Unable to find ExtendedSocketOptions.SO_INCOMING_NAPI_ID, make sure you run a JDK version 15+"));
        try {
            return (SocketOption<Integer>) napiIdField.get(null);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to get socket option ExtendedSocketOptions.SO_INCOMING_NAPI_ID", e);
        }
    }

    @Override
    public IOWorker selectIOWorker(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
        int ioWorkerArrayIndexId = getIOWorkerArrayIndex(networkChannel, ioWorkers);
        if (ioWorkerArrayIndexId >= 0) {
            IOWorker selected = ioWorkers[ioWorkerArrayIndexId];
            log.info("NetworkChannel {} assigned to IOWorker {}", networkChannel, selected.getName());
            return selected;
        }
        return FALLBACK.selectIOWorker(networkChannel, ioWorkers);
    }

    @Override
    public List<SessionMove> rebalance(IOWorker[] ioWorkers) {
        List<SessionMove> moves = new ArrayList<>();
        for (int currentIdx = 0; currentIdx < ioWorkers.length; currentIdx++) {
            IOWorker current = ioWorkers[currentIdx];
            for (IOSession session : current.getRegisteredSessions()) {
                NetworkChannel channel = session.getNetworkChannel();
                if (channel == null) {
                    continue;
                }
                int ioWorkerArrayIndexId = getIOWorkerArrayIndex(channel, ioWorkers);
                if (ioWorkerArrayIndexId >= 0 && ioWorkerArrayIndexId != currentIdx) {
                    moves.add(new SessionMove(session, ioWorkers[ioWorkerArrayIndexId]));
                }
            }
        }
        return moves;
    }

    private int getIOWorkerArrayIndex(NetworkChannel networkChannel, IOWorker[] ioWorkers) {
        try {
            int socketRXQueueId = networkChannel.getOption(NAPID_SO);
            if (socketRXQueueId == 0) {
                // 0 when the socket is not bound or not traffic has been received yet
                log.debug("NAPI ID for channel {} is not yet assigned", NAPID_SO);
            } else {
                log.debug("NetworkChannel {} has RX queue Id {}", networkChannel, socketRXQueueId);
                int ioWorkerArrayIndexId = socketRXQueueId - 1;
                if (ioWorkerArrayIndexId >= 0 && ioWorkerArrayIndexId < ioWorkers.length) {
                    return ioWorkerArrayIndexId;
                }
            }

        } catch (IOException e) {
            log.error("Failed to retrieve SO_INCOMING_NAPI_ID", e);
        }
        return -1;
    }
}
