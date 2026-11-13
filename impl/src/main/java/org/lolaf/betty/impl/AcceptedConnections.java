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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.ServerBuilder;

import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Set;

/**
 * The connections a server holds open, from their accept until their socket closes, whichever path closes it. Only the
 * acceptor's IO thread uses it.
 */
@Slf4j
@RequiredArgsConstructor
final class AcceptedConnections {

    @Getter
    private final ServerBuilder serverBuilder;
    private final Set<SocketChannel> open = new HashSet<>();
    private boolean refusing;

    boolean admit(SocketChannel socket) {
        int maxSessions = serverBuilder.getMaxSessions();
        if (open.size() >= maxSessions) {
            // a closed socket frees its slot, so only a full server looks for one
            open.removeIf(channel -> !channel.isOpen());
        }
        if (open.size() >= maxSessions) {
            if (!refusing) {
                refusing = true;
                log.warn("Server '{}' refuses new connections: it holds {}, its maxSessions", serverBuilder.getId(), maxSessions);
            }
            return false;
        }
        if (refusing) {
            refusing = false;
            log.info("Server '{}' accepts new connections again", serverBuilder.getId());
        }
        open.add(socket);
        return true;
    }
}
