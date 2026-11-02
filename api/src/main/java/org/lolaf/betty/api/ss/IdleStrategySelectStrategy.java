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
package org.lolaf.betty.api.ss;

import lombok.Value;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.idling.WaitNotifyIdleStrategy;
import org.lolaf.ringos.idling.YieldingIdleStrategy;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Polls the selector and never blocks, handing the waiting to a ringos {@link IdleStrategy}.
 * <p>
 * This is the low-latency option: readiness is seen on the next pass rather than after a wakeup, and a
 * cross-thread write calls {@link IdleStrategy#wakeup()}, which cuts a park short without a selector syscall. The cost
 * is a core - a busy-spinning idle strategy holds one whether or not there is traffic - so it suits a thread
 * dedicated to sessions that matter, not a general pool. Readiness itself never ends a park: a parking strategy
 * still delays inbound data by up to its park period.
 */
@Value
public class IdleStrategySelectStrategy implements SelectStrategy {

    /**
     * Consulted after every pass with the number of keys that were ready, so it can spin, yield or park according
     * to how busy the thread has been.
     */
    IdleStrategy idleStrategy;

    /**
     * @param idleStrategy how the IO thread waits between passes
     * @throws NullPointerException     if {@code idleStrategy} is {@code null}
     * @throws IllegalArgumentException if it is a {@link WaitNotifyIdleStrategy}, which parks with no timeout
     *                                  and so would never see inbound data on a quiet thread
     */
    public IdleStrategySelectStrategy(IdleStrategy idleStrategy) {
        Objects.requireNonNull(idleStrategy, "idleStrategy");
        if (idleStrategy instanceof WaitNotifyIdleStrategy) {
            throw new IllegalArgumentException(idleStrategy.getClass().getSimpleName() + " cannot drive a select "
                    + "loop: it parks until wakeup(), and incoming data wakes nothing, so the IO thread would stop "
                    + "polling. Use BusySpinIdleStrategy, YieldingIdleStrategy, BackoffIdleStrategy or "
                    + "TimedWaitNotifyIdleStrategy, or WakeupSelectStrategy if the point is to block");
        }
        this.idleStrategy = idleStrategy;
    }

    @Override
    public Runnable wakeupAction(Selector selector) {
        if (idleStrategy instanceof BusySpinIdleStrategy || idleStrategy instanceof YieldingIdleStrategy) {
            return null;
        }
        return idleStrategy::wakeup;
    }

    @Override
    public void assignToThread(Thread thread) {
        idleStrategy.assignToThread(thread);
    }

    @Override
    public int select(Selector selector, Consumer<SelectionKey> selectionKeyConsumer) throws IOException {
        int selectedKeys = selector.selectNow(selectionKeyConsumer);
        idleStrategy.idle(selectedKeys);
        return selectedKeys;
    }

}