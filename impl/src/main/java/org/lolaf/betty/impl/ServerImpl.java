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

import org.lolaf.betty.api.io.*;
import org.lolaf.ringos.Deadline;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.Startable;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.util.function.Predicate;

@Slf4j
class ServerImpl extends Startable.SimpleStartable<Server> implements Server {

    @Getter
    private final ServerBuilder builder;
    private final InetSocketAddress bindAddress;
    @Getter(AccessLevel.PACKAGE)
    private final IOWorkersGroup ioWorkersGroup;
    private final IOWorkersGroup acceptorWorkersGroup;
    private final Set<IOSession> connectedSessions;
    private final IOBufferPool sharedIOBufferPool;
    private IOSession[] connectedSessionsArray;
    private ServerSocketChannel serverSocket;
    private SelectionKey acceptKey;

    private ServerImpl(ServerBuilder serverBuilder) {
        this.bindAddress = serverBuilder.getBindAddress();
        this.ioWorkersGroup = Optional.ofNullable(serverBuilder.getIoWorkersGroup())
                .orElseGet(() -> IOWorkersGroupSettings.builder()
                        .id(serverBuilder.getId())
                        .build().newInstance());
        this.acceptorWorkersGroup = Optional.ofNullable(serverBuilder.getAcceptorIoWorkerGroup()).orElse(ioWorkersGroup);
        this.builder = serverBuilder.toBuilder()
                .ioEventsListener(getIOSessionEventsListenerProxy(serverBuilder.getIoEventsListener()))
                .ioWorkersGroup(this.ioWorkersGroup)
                .build();
        this.connectedSessions = new HashSet<>();
        this.connectedSessionsArray = new IOSession[0];
        IOBufferPoolSettings ioBufferPoolSettings = serverBuilder.getIoSessionsSharedWriteIoBufferPoolSettings();
        if (ioBufferPoolSettings != null && !ioBufferPoolSettings.getMultiThreadingAccessMode()
                .equals(IOBufferPoolSettings.MultiThreadingAccessMode.MULTIPLE_BORROWER_AND_RETURN_TO_POOL_THREADS)) {
            ioBufferPoolSettings = ioBufferPoolSettings.toBuilder()
                    .multiThreadingAccessMode(IOBufferPoolSettings.MultiThreadingAccessMode.MULTIPLE_BORROWER_AND_RETURN_TO_POOL_THREADS).build();
        }
        this.sharedIOBufferPool = ioBufferPoolSettings != null ? ioBufferPoolSettings.newInstance() : null;
    }

    private static void sendMessageToSession(ByteBuffer message, IOSession ioSession) {
        ioSession.send(ioSession.allocate(message.limit()).put(message.position(0)), true);
    }

    @Override
    public List<IOSession> getIOSessions() {
        synchronized (connectedSessions) {
            return new ArrayList<>(connectedSessions);
        }
    }

    @Override
    public void enableIOStats(boolean enabled) {
        synchronized (connectedSessions) {
            connectedSessions.forEach(s -> ((IOSessionImpl) s).toggleIoStats(enabled));
        }
    }

    @Override
    public void enableIOWorkerStats(boolean enabled) {
        ioWorkersGroup.toggleIoWorkerStats(enabled);
    }

    @Override
    public void enableIOBufferPoolStats(boolean enabled) {
        synchronized (connectedSessions) {
            connectedSessions.forEach(s -> ((IOSessionImpl) s).toggleIOBufferPoolStats(enabled));
        }
    }

    @Override
    public int broadcast(ByteBuffer message, IOSessionTag... tags) {
        int matches = 0;
        IOSession[] connectedSessionsArrayLocal = connectedSessionsArray;
        for (IOSession ioSession : connectedSessionsArrayLocal) {
            if (tags == null || ioSession.matchesTags(tags)) {
                sendMessageToSession(message, ioSession);
                matches++;
            }
        }
        return matches;
    }

    @Override
    public int broadcast(ByteBuffer message, IOSessionTag tag) {
        int matches = 0;
        IOSession[] connectedSessionsArrayLocal = connectedSessionsArray;
        for (IOSession ioSession : connectedSessionsArrayLocal) {
            if (ioSession.matchesTag(tag)) {
                sendMessageToSession(message, ioSession);
                matches++;
            }
        }
        return matches;
    }

    @Override
    public int broadcast(ByteBuffer message, Predicate<IOSession> targetSession) {
        int matches = 0;
        IOSession[] connectedSessionsArrayLocal = connectedSessionsArray;
        for (IOSession ioSession : connectedSessionsArrayLocal) {
            if (targetSession.test(ioSession)) {
                sendMessageToSession(message, ioSession);
                matches++;
            }
        }
        return matches;
    }

    @Override
    protected void startMe() throws StartStopException {
        try {
            if (sharedIOBufferPool != null) {
                sharedIOBufferPool.start(null);
                SharedIOBufferPoolInstanceRegistry.getInstance().addPool(builder.getId(), sharedIOBufferPool);
            }
            serverSocket = ServerSocketChannel.open();
            // before bind, which is the only time it has any effect: set afterwards it is silently a no-op, and the
            // bind fails with "Address already in use" whenever the port still carries connections in TIME_WAIT from
            // a server that has just been stopped - restarting on a fixed port, or a test rebinding one
            serverSocket.setOption(StandardSocketOptions.SO_REUSEADDR, true);
            serverSocket.bind(bindAddress);
            serverSocket.configureBlocking(false);
            acceptorWorkersGroup.start();
            ioWorkersGroup.start();
            acceptKey = serverSocket.register(getSocketAcceptSelector().orElseThrow(), SelectionKey.OP_ACCEPT, builder);
        } catch (IOException ex) {
            throw new StartStopException(ex);
        }
        log.info("Server '{}' started on {}", builder.getId(), bindAddress);
    }


    @Override
    public Server stopListening() {
        if (acceptKey != null) {
            acceptKey.cancel();
            acceptKey = null;
        }
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                log.info("Failed to close server '{}' socket ", builder.getId(), e);
            }
            serverSocket = null;
            log.info("Server '{}' stopped listening incoming connections", builder.getId());
        }
        return this;
    }

    @Override
    protected void stopMe(Deadline stopDeadline) throws StartStopException {
        stopListening();

        getIOSessions().forEach(s -> s.stop(stopDeadline));
        acceptorWorkersGroup.stop(stopDeadline);
        ioWorkersGroup.stop(stopDeadline);
        connectedSessions.clear();
        connectedSessionsArray = connectedSessions.toArray(new IOSession[0]);
        if (sharedIOBufferPool != null) {
            sharedIOBufferPool.stop();
            SharedIOBufferPoolInstanceRegistry.getInstance().removePool(builder.getId());
        }
        log.info("Server '{}' stopped", builder.getId());
    }

    private Optional<Selector> getSocketAcceptSelector() {
        return Optional.ofNullable(acceptorWorkersGroup.getNext(serverSocket)).map(IOWorker::getSelector);
    }

    private IOEventsListener getIOSessionEventsListenerProxy(IOEventsListener proxy) {
        if (proxy == null) {
            throw new IllegalStateException("Missing IOEventsListener in server settings");
        }
        return new FailSafeIOEventsListener(proxy) {
            @Override
            public void onConnected(IOSession session) {
                log.info("Connection '{}' from {}", builder.getId(), session.getSocketAddress());
                synchronized (connectedSessions) {
                    connectedSessions.add(session);
                    connectedSessionsArray = connectedSessions.toArray(new IOSession[0]);
                }
                super.onConnected(session);
            }

            @Override
            public void onDisconnected(IOSession session) {
                if (session != null) {
                    log.info("Disconnection '{}' from {}", builder.getId(), session.getSocketAddress());
                    synchronized (connectedSessions) {
                        connectedSessions.remove(session);
                        connectedSessionsArray = connectedSessions.toArray(new IOSession[0]);
                    }
                    super.onDisconnected(session);
                }
            }
        };
    }

    public static class ServerFactoryImpl implements Server.ServerFactory {

        @Override
        public Server newInstance(ServerBuilder settings) {
            return new ServerImpl(settings);
        }
    }
}