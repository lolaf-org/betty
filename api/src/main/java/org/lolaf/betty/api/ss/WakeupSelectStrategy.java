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

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.util.function.Consumer;

/**
 * Blocks on the selector until something is ready or the timeout expires, giving the core back while it waits.
 * <p>
 * The default, and the right choice unless a core can be spent on one IO thread. A cross-thread write has to wake
 * the selector to be seen, so it costs a syscall that {@link IdleStrategySelectStrategy} does not.
 */
public class WakeupSelectStrategy implements SelectStrategy {

    private final int selectTimeoutMillis;

    /**
     * The timeout bounds how long an idle thread sleeps, so it is also the worst case for anything the wakeup
     * misses; it is not the latency of ordinary traffic, which arrives as readiness.
     *
     * @param selectTimeoutMillis how long a select blocks for, in milliseconds
     * @throws IllegalArgumentException if it is not greater than zero
     */
    public WakeupSelectStrategy(int selectTimeoutMillis) {
        this.selectTimeoutMillis = selectTimeoutMillis;
        if (selectTimeoutMillis <= 0) {
            throw new IllegalArgumentException("selectTimeoutMillis should be greater than zero");
        }
    }

    @Override
    public boolean requireSelectorWakeup() {
        return true;
    }

    @Override
    public int select(Selector selector, Consumer<SelectionKey> selectionKeyConsumer) throws IOException {
        return selector.select(selectionKeyConsumer, selectTimeoutMillis);
    }
}