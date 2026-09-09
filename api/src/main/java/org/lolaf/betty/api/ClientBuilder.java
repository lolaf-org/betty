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
package org.lolaf.betty.api;

import org.lolaf.betty.api.settings.InstanceProvider;
import org.lolaf.betty.api.settings.SSLSettings;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * The settings a {@link Client} is built from, and the entry point to the library on the client side:
 * {@link #newInstance()} is what finds an implementation.
 */
@Getter
@SuperBuilder(toBuilder = true)
public class ClientBuilder extends BaseBuilder implements InstanceProvider<Client> {

    /**
     * A list of addresses to connect, will connect to the first host responding in the list
     */
    @Singular
    private final List<InetSocketAddress> connectAddresses;

    /**
     * Runs the connection attempts, and nothing else. When null the client creates a single-threaded one and shuts
     * it down with itself; one supplied here is the caller's and is left running.
     */
    private final ScheduledExecutorService scheduledExecutorService;

    /**
     * TLS for the connection, or null for a plain socket.
     */
    private final SSLSettings SSLSettings;

    /**
     * Delay between connection attempts, and before the first one - {@link Client#start()} arms the attempt rather
     * than making it, so this is also how long a start takes to reach the network.
     */
    @Builder.Default
    private final Duration connectionRetry = Duration.ofSeconds(1);

    @Override
    public Client newInstance() {
        return InstanceProvider.getSpiInstance(this, Client.ClientFactory.class);
    }
}
