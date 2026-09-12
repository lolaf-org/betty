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
 * How an IO thread waits for its selector, which is the single biggest latency-versus-CPU choice in betty.
 * <p>
 * A strategy that blocks gives the core back and pays a wakeup to get it; one that polls keeps the core hot and
 * sees readiness immediately. {@link WakeupSelectStrategy} is the first, {@link IdleStrategySelectStrategy} the
 * second, and the two are not otherwise different.
 */
public interface SelectStrategy {

    /**
     * Whether a thread that queues work for this session must wake the selector for it to be noticed.
     * <p>
     * True for a blocking select, which would otherwise sit until its timeout; false for a polling one, which
     * comes round on its own. Answering true when it is not needed costs a syscall per cross-thread write, and
     * answering false when it is needed stalls that write until the select times out.
     *
     * @return true if {@code Selector.wakeup()} is required
     */
    boolean requireSelectorWakeup();

    /**
     * Runs one selection pass, and is where the strategy's waiting happens.
     *
     * @param selector            the worker's selector
     * @param selectionKeyConsumer handed each ready key; called on the IO thread
     * @return how many keys were ready
     * @throws IOException if the selection fails
     */
    int select(Selector selector, Consumer<SelectionKey> selectionKeyConsumer) throws IOException;

    /**
     * Applies whatever thread-level setup this strategy needs, called by the IO thread on itself before its
     * select loop starts.
     * <p>
     * Most strategies need none, hence the empty default. {@link IdleStrategySelectStrategy} passes it on to its
     * {@code IdleStrategy}, which is how a strategy that narrows the OS timer slack gets to do it on the thread
     * that will actually park - the setting applies to the calling thread, so no other thread can do it for it.
     *
     * @param thread the IO thread that will run the select loop
     */
    default void assignToThread(Thread thread) {
        // nothing to do
    }
}
