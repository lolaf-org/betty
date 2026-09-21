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
import java.util.concurrent.*;
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
        requestConnect();
        log.info("IO client '{}' is started", clientBuilder.getId());
        return this;
    }

    @Override
    public ClientImpl stop(Deadline stopDeadline) throws StartStopException {
        if (!started.getAndSet(false)) {
            return this;
        }
        // a pending attempt already finds the client stopped; cancelling it just frees the scheduler, which may be
        // shared with the application and so is never interrupted
        try {
            scheduledExecutorService.execute(this::cancelConnectTask);
        } catch (RejectedExecutionException ex) {
            log.debug("Client '{}' scheduler already stopped", clientBuilder.getId());
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


    private void requestConnect() {
        if (!started.get()) {
            return;
        }
        try {
            scheduledExecutorService.execute(this::scheduleConnectTaskIfNone);
        } catch (RejectedExecutionException ex) {
            log.debug("Client '{}' is stopping, not reconnecting", clientBuilder.getId());
        }
    }

    private void scheduleConnectTaskIfNone() {
        if (!started.get() || connectedSession.get() != null || (connectTask != null && !connectTask.isDone())) {
            return;
        }
        connectTask = scheduledExecutorService
                .schedule(this::connectTask, clientBuilder.getConnectionRetry().toMillis(), TimeUnit.MILLISECONDS);
    }

    private void cancelConnectTask() {
        if (connectTask != null) {
            connectTask.cancel(false);
            connectTask = null;
        }
    }

    private void connectTask() {
        // this run is the pending task: consumed, so that re-arming below is not taken for a second chain
        connectTask = null;
        if (!started.get() || connectedSession.get() != null) {
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
            scheduleConnectTaskIfNone();
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
                try {
                    scheduledExecutorService.execute(ClientImpl.this::cancelConnectTask);
                } catch (RejectedExecutionException ex) {
                    log.debug("Client '{}' is stopping", clientBuilder.getId());
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
                requestConnect();
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