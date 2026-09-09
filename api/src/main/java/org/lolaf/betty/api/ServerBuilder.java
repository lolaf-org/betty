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

import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.InstanceProvider;
import org.lolaf.betty.api.settings.ServerSSLSettings;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import java.net.InetSocketAddress;


/**
 * The settings a {@link Server} is built from, and the entry point to the library on the server side:
 * {@link #newInstance()} is what finds an implementation.
 */
@Getter
@SuperBuilder(toBuilder = true)
public class ServerBuilder extends BaseBuilder implements InstanceProvider<Server> {

    /**
     * TLS for accepted connections, or null to serve plain sockets.
     */
    private final ServerSSLSettings serverSSLSettings;

    /**
     * The address to listen on.
     */
    private final InetSocketAddress bindAddress;

    /**
     * IO worker group for accepting connections, if null {@link BaseBuilder#getIoWorkersGroup()} will be used to accept connections.
     * It is encouraged to define one when using SSL (as SSL handshake will be processed into an IO worker) or constant low latency is desired to process IO read/writes
     */
    private final IOWorkersGroup acceptorIoWorkerGroup;

    /**
     * IO session shared write buffer pool settings, if set (non null) IO sessions will not use their own IO buffer pool {@link IOSettings#getWriteIoBufferPoolSettings()}
     * but this shared pool. This allows better IO buffers resources management at the cost of more cache misses and synchronization needed when accessing the pool as it will be accessed by multiple IO threads
     */
    private final IOBufferPoolSettings ioSessionsSharedWriteIoBufferPoolSettings;

    /**
     * Vets each accepted connection; the default accepts every one.
     */
    @Builder.Default
    private final RemoteSessionsFilter remoteSessionsFilter = (remoteAddress, remoteSessionCertificate) -> true;

    @Override
    public Server newInstance() {
        return InstanceProvider.getSpiInstance(this, Server.ServerFactory.class);
    }
}
