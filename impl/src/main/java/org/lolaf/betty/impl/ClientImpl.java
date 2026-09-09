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

import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
class ClientImpl implements Client {

    private final ClientBuilder clientBuilder;
    private final AtomicBoolean started;
    private final IOWorkersGroup ioWorkersGroup;
    private final AtomicReference<IOSession> connectedSession;
    private ScheduledFuture<?> connectTask;
    private Iterator<InetSocketAddress> connectAddressIterator;
    private InetSocketAddress nextConnectAddress;
    private ScheduledExecutorService scheduledExecutorService;
    private boolean embeddedScheduledExecutorService;

    private ClientImpl(ClientBuilder clientBuilder) {
        this.clientBuilder = clientBuilder.toBuilder()
                .ioEventsListener(getIOSessionEventsListenerProxy(clientBuilder.getIoEventsListener()))
                .build();
        this.started = new AtomicBoolean();
        this.ioWorkersGroup = Optional.ofNullable(clientBuilder.getIoWorkersGroup())
                .orElseGet(() -> IOWorkersGroupSettings.builder()
                        .id(clientBuilder.getId())
                        .build().newInstance());
        this.connectedSession = new AtomicReference<>();
    }

    @Override
    public boolean isConnected() {
        return connectedSession.get() != null;
    }

    @Override
    public ClientBuilder getBuilder() {
        return clientBuilder;
    }

    @Override
    public IOSession getIOSession() {
        return connectedSession.get();
    }

    @Override
    public void enableIOWorkerStats(boolean enabled) {
        ioWorkersGroup.toggleIoWorkerStats(enabled);
    }

    @Override
    public void enableIOStats(boolean enabled) {
        IOSessionImpl ioSessionImpl = ((IOSessionImpl) connectedSession.get());
        if (ioSessionImpl != null) {
            ioSessionImpl.toggleIoStats(enabled);
        }
    }

    @Override
    public void enableIOBufferPoolStats(boolean enabled) {
        IOSessionImpl ioSessionImpl = ((IOSessionImpl) connectedSession.get());
        if (ioSessionImpl != null) {
            ioSessionImpl.toggleIOBufferPoolStats(enabled);
        }
    }

    @Override
    public ClientImpl start() {
        if (started.getAndSet(true)) {
            return this;
        }
        scheduledExecutorService = clientBuilder.getScheduledExecutorService();
        if (scheduledExecutorService == null) {
            scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "betty-client-" + clientBuilder.getId() + "-scheduler"));
            embeddedScheduledExecutorService = true;
        }
        ioWorkersGroup.start();
        connectAddressIterator = clientBuilder.getConnectAddresses().iterator();
        nextConnectAddress = null;
        scheduleConnectTask();
        log.info("IO client '{}' is started", clientBuilder.getId());
        return this;
    }

    @Override
    public ClientImpl stop(Deadline stopDeadline) throws StartStopException {
        if (!started.getAndSet(false)) {
            return this;
        }
        if (connectTask != null && !connectTask.isCancelled()) {
            if (!connectTask.cancel(true)) {
                log.info("Client '{}' failed to cancel connection task", clientBuilder.getId());
            }
            connectTask = null;
        }
        IOSession ioSession = connectedSession.getAndSet(null);
        if (ioSession != null) {
            ioSession.stop(stopDeadline.fromRemainingTime(0.8));
        }
        ioWorkersGroup.stop(stopDeadline);
        if (embeddedScheduledExecutorService) {
            scheduledExecutorService.shutdownNow();
        }
        log.info("Client '{}' is stopped", clientBuilder.getId());
        return this;
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    private void scheduleConnectTask() {
        if (started.get()) {
            connectTask = scheduledExecutorService
                    .schedule(this::connectTask, clientBuilder.getConnectionRetry().toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private void connectTask() {
        if (!started.get()) {
            return;
        }
        if (!connectAddressIterator.hasNext()) {
            connectAddressIterator = clientBuilder.getConnectAddresses().iterator();
        }
        if (nextConnectAddress == null) {
            nextConnectAddress = connectAddressIterator.next();
        }
        if (!clientBuilder.getIoEventsListener().isReadyToConnect(nextConnectAddress)) {
            // held back rather than dropped: the address stays the candidate for the next attempt, so waiting does
            // not walk through the addresses, and the retry is re-armed so the listener is asked again
            log.debug("Client '{}' is not ready to connect to {}, retrying later", clientBuilder.getId(), nextConnectAddress);
            scheduleConnectTask();
            return;
        }
        try {
            InetSocketAddress connectAddress = nextConnectAddress;
            nextConnectAddress = null;
            log.debug("Connecting '{}' to {}", clientBuilder.getId(), connectAddress);
            SocketChannel clientSocket = SocketChannel.open();
            clientSocket.configureBlocking(false);
            IOWorker ioWorker = ioWorkersGroup.getNext(clientSocket);
            if (!clientSocket.connect(connectAddress)) {
                clientSocket.register(ioWorker.getSelector(), SelectionKey.OP_CONNECT, clientBuilder);
            } else {
                ioWorker.register(true, clientSocket, clientBuilder);
            }
        } catch (Exception e) {
            log.info("Failed to process connect task", e);
        }
    }

    private IOEventsListener getIOSessionEventsListenerProxy(IOEventsListener proxy) {
        if (proxy == null) {
            throw new IllegalStateException("Missing IOEventsListener in client settings");
        }
        return new FailSafeIOEventsListener(proxy) {
            @Override
            public void onConnected(IOSession session) {
                log.info("Connected '{}' to {}", clientBuilder.getId(), session.getSocketAddress());
                connectedSession.set(session);
                if (connectTask != null) {
                    connectTask.cancel(true);
                    connectTask = null;
                }
                super.onConnected(session);
            }

            @Override
            public void onDisconnected(IOSession session) {
                // null when a failed connection occurred
                if (session != null) {
                    log.info("Disconnected '{}' from {}", clientBuilder.getId(), session.getSocketAddress());
                    super.onDisconnected(session);
                }
                connectedSession.set(null);
                scheduleConnectTask();
            }
        };
    }

    public static class ClientFactoryImpl implements Client.ClientFactory {

        @Override
        public Client newInstance(ClientBuilder settings) {
            return new ClientImpl(settings);
        }
    }
}