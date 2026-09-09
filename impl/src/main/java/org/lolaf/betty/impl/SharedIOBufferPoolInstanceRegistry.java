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

import org.lolaf.betty.api.io.IOBufferPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Where a server's shared write buffer pool lives so that every session it accepts finds the same one, keyed by
 * server id.
 * <p>
 * The id is therefore load-bearing rather than cosmetic: two servers sharing one is rejected at registration
 * instead of silently sharing a pool neither was sized for.
 */
public class SharedIOBufferPoolInstanceRegistry {

    private static final SharedIOBufferPoolInstanceRegistry INSTANCE = new SharedIOBufferPoolInstanceRegistry();

    private final Map<String, IOBufferPool> poolInstancePerServerId;

    private SharedIOBufferPoolInstanceRegistry() {
        poolInstancePerServerId = new ConcurrentHashMap<>();
    }

    /**
     * The registry; one per JVM, since a server id is meant to be unique within it.
     *
     * @return the singleton
     */
    public static SharedIOBufferPoolInstanceRegistry getInstance() {
        return INSTANCE;
    }

    /**
     * Registers a server's shared pool.
     *
     * @param serverId the server's id
     * @param pool     its shared pool
     * @throws IllegalStateException if that id is already registered, which means two servers were built with
     *                               the same id rather than that one was started twice
     */
    public void addPool(String serverId, IOBufferPool pool) {
        if (poolInstancePerServerId.putIfAbsent(serverId, pool) != null) {
            throw new IllegalStateException("Registry already contains a shared pool, did you use same server id for 2 different instance ?");
        }
    }

    /**
     * The pool registered for a server.
     *
     * @param serverId the server's id
     * @return its pool, or null if it has none - which is the ordinary case, a shared pool being opt-in
     */
    public IOBufferPool getPool(String serverId) {
        return poolInstancePerServerId.get(serverId);
    }

    /**
     * Drops a server's registration, so the id can be reused once it has stopped.
     *
     * @param serverId the server's id
     */
    public void removePool(String serverId) {
        poolInstancePerServerId.remove(serverId);
    }
}