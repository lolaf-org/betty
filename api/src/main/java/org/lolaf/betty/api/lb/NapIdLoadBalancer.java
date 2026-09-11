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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

/**
 * NIC-aware {@link IOWorkerLoadBalancer} that groups the sessions fed by the same NIC RX queue onto the
 * same {@link IOWorker}, using the kernel's NAPI ID from {@code ExtendedSocketOptions.SO_INCOMING_NAPI_ID}.
 * Their inbound packets are then handed to one IO thread instead of being scattered across workers, so the
 * queue's softirq context has a single consumer. Landing that thread on the CPU serving the queue's IRQ is
 * an operator decision: pin the workers, because the NAPI ID names a queue and says nothing about a CPU.
 *
 * <p><b>Placement.</b> {@link #selectIOWorker(NetworkChannel, IOWorker[])} reads {@code SO_INCOMING_NAPI_ID}
 * and resolves it through a slot registry — each distinct NAPI ID takes the next slot on first sight, and
 * the worker is {@code slot % ioWorkers.length}. The registry is needed because a NAPI ID is not a queue
 * index: the kernel allocates it from a global counter that starts above {@code NR_CPUS} so it can never be
 * confused with a CPU id, which on a distro kernel built with {@code CONFIG_NR_CPUS=8192} means the first
 * queue reports 8193. The ID is {@code 0} until the socket has received traffic, in which case placement
 * falls back to {@link MinRegisteredSessionLoadBalancer} and the periodic {@link #rebalance(IOWorker[])}
 * corrects it once the queue is known.
 *
 * <p><b>Rebalancing.</b> {@link #rebalance(IOWorker[])} walks every registered session, re-reads its NAPI
 * ID, and emits a {@link SessionMove} whenever the implied worker differs from the current owner — fixing
 * up initial placements that landed on the fallback worker and following any NIC-side rebalancing of RX
 * queues over the session's lifetime.
 *
 * <p><b>Requirements.</b> JDK 15+ for {@code ExtendedSocketOptions.SO_INCOMING_NAPI_ID}, and a Linux kernel
 * and driver that expose NAPI IDs. Loopback reports {@code 0}, so over loopback this balancer only ever
 * uses its fallback.
 *
 * <p>Singleton — obtain via {@link #getInstance()}. The slot registry is shared deliberately, so an RX queue
 * keeps one slot across every worker group in the JVM.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class NapIdLoadBalancer implements IOWorkerLoadBalancer {

    private static final SocketOption<Integer> NAPID_SO = getNapIdSo();
    private static final MinRegisteredSessionLoadBalancer FALLBACK = MinRegisteredSessionLoadBalancer.getInstance();
    private static final NapIdLoadBalancer INSTANCE = new NapIdLoadBalancer();

    private volatile int[] napIdsBySlot = new int[0];

    /**
     * The shared instance; the slot registry it holds is shared deliberately.
     *
     * @return the singleton
     */
    public static NapIdLoadBalancer getInstance() {
        return INSTANCE;
    }

    /**
     * The NAPI ID the kernel reports for a channel, {@code 0} when it has none — nothing has been received on the
     * socket yet, or the packets never came off a NIC queue, as with anything delivered locally. The ID identifies
     * the RX queue, never a CPU, and is only comparable to another ID read on the same host.
     *
     * @param networkChannel the channel to query
     * @return the NAPI ID, or {@code 0}
     * @throws IOException if the option cannot be read
     */
    public static int napIdOf(NetworkChannel networkChannel) throws IOException {
        return networkChannel.getOption(NAPID_SO);
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
        if (ioWorkers.length == 0) {
            return -1;
        }
        try {
            int napId = napIdOf(networkChannel);
            if (napId == 0) {
                // 0 when the socket is not bound or no traffic has been received yet
                log.debug("NAPI ID for channel {} is not yet assigned", networkChannel);
                return -1;
            }
            int slot = slotOf(napId);
            log.debug("NetworkChannel {} has RX queue Id {} in slot {}", networkChannel, napId, slot);
            return slot % ioWorkers.length;
        } catch (IOException e) {
            log.error("Failed to retrieve SO_INCOMING_NAPI_ID", e);
            return -1;
        }
    }

    private int slotOf(int napId) {
        int[] napIds = napIdsBySlot;
        for (int slot = 0; slot < napIds.length; slot++) {
            if (napIds[slot] == napId) {
                return slot;
            }
        }
        return registerSlot(napId);
    }

    private synchronized int registerSlot(int napId) {
        int[] napIds = napIdsBySlot;
        for (int slot = 0; slot < napIds.length; slot++) {
            if (napIds[slot] == napId) {
                return slot;
            }
        }
        int[] grown = Arrays.copyOf(napIds, napIds.length + 1);
        grown[napIds.length] = napId;
        napIdsBySlot = grown;
        return napIds.length;
    }
}
