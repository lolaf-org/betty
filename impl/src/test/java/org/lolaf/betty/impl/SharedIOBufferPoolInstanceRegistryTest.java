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
package org.lolaf.betty.impl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.lolaf.betty.api.io.IOBufferPool;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * The registry rejects a duplicate server id so two servers cannot silently share a pool neither was sized for.
 * These are the contract, not a proof of the {@code putIfAbsent} that carries it: check-then-act passes them too,
 * and only loses the race under contention - measured at 11 double registrations in 5000 rounds of two threads.
 */
class SharedIOBufferPoolInstanceRegistryTest {

    private final String serverId = getClass().getSimpleName() + "-server";

    @AfterEach
    void removeThePool() {
        SharedIOBufferPoolInstanceRegistry.getInstance().removePool(serverId);
    }

    @Test
    void rejectsASecondPoolForTheSameServerIdAndKeepsTheFirst() {
        SharedIOBufferPoolInstanceRegistry registry = SharedIOBufferPoolInstanceRegistry.getInstance();
        IOBufferPool first = mock(IOBufferPool.class);
        IOBufferPool second = mock(IOBufferPool.class);

        registry.addPool(serverId, first);

        assertThatThrownBy(() -> registry.addPool(serverId, second))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("already contains a shared pool");
        assertThat(registry.getPool(serverId)).isSameAs(first);
    }

    @Test
    void makesTheIdAvailableAgainOnceThePoolIsRemoved() {
        SharedIOBufferPoolInstanceRegistry registry = SharedIOBufferPoolInstanceRegistry.getInstance();
        IOBufferPool first = mock(IOBufferPool.class);
        IOBufferPool second = mock(IOBufferPool.class);

        registry.addPool(serverId, first);
        registry.removePool(serverId);
        registry.addPool(serverId, second);

        assertThat(registry.getPool(serverId)).isSameAs(second);
    }
}
