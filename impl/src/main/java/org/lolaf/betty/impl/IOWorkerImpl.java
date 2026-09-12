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
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.BaseBuilder;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.RemoteSessionsFilter;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOBufferPool;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.settings.SSLSettings;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.stats.IOWorkerStats;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory;

import javax.net.ssl.SSLHandshakeException;
import java.io.EOFException;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@Slf4j
class IOWorkerImpl implements IOWorker {

    private static final long ZERO = 0L;
    private final AtomicBoolean running;
    @Getter
    private final String name;
    @Getter
    private final String group;
    private final Set<IOSessionImpl> registeredSessions;
    private final SelectStrategy selectStrategy;
    private final IOWorkersGroupSettings ioWorkersGroupSettings;
    private final Consumer<SelectionKey> selectKeyConsumer;
    private final IOWorkerStats ioWorkerStatistics;
    private final Runnable statsTask;
    private final Map<IOSession, RingBufferStates> ioSessionRingBufferStates;
    private final Thread ioWorkerThread;
    private final ExponentialMovingAverage loadEma;
    private final RingBuffer<IOWorkerMigrationCommand> pendingMigrations;
    @Getter
    private Selector selector;
    private Selector optimizedSelector;
    private boolean trackReceiveTime;
    private long localReceiveTime;
    private long acceptCpuTimeInNanos;
    private long connectCpuTimeInNanos;
    private int selectedKeysCounter;
    private int pendingHandshakesCount;
    private boolean enabledStatistics;
    private volatile boolean hasPendingMigrations;
    private Runnable selectorWakeupIfNeeded;

    IOWorkerImpl(String name, IOWorkersGroupSettings ioWorkersGroupSettings, IOWorkersGroupSettings.IOThreadGroup threadGroup) {
        this.registeredSessions = Collections.synchronizedSet(new HashSet<>());
        this.name = "IO-worker-" + name;
        this.group = threadGroup.getName();
        this.running = new AtomicBoolean(false);
        this.selectStrategy = threadGroup.getSelectStrategy();
        this.ioWorkersGroupSettings = ioWorkersGroupSettings;
        this.selectKeyConsumer = this::onSelectionKey;
        this.ioWorkerStatistics = ioWorkersGroupSettings.getIoWorkerStatisticsProvider().get(this);
        boolean loadBalancerRequiresLoadComputation = ioWorkersGroupSettings.getIoWorkerLoadBalancer().requiresIOWorkersLoadComputation();
        this.enabledStatistics = ioWorkerStatistics.enabledByDefault() || loadBalancerRequiresLoadComputation;
        this.loadEma = loadBalancerRequiresLoadComputation
                ? ExponentialMovingAverage.fromTimeWindow(ioWorkersGroupSettings.getWorkersLoadEMATimeWindow(), ioWorkerStatistics.getStatsResolution()) : null;
        this.statsTask = this::runStats;
        this.ioSessionRingBufferStates = new ConcurrentHashMap<>();
        this.pendingMigrations = RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER, 8, IOWorkerMigrationCommand::new);
        ioWorkerThread = threadGroup.getThreadFactory().newThread(this::runLoop);
        ioWorkerThread.setName(this.name);
        ioWorkerThread.setDaemon(true);
        ioWorkerThread.setUncaughtExceptionHandler((t, e) -> log.error("CRITICAL: uncaught exception in IO Thread {}", t.getName(), e));
    }


    @Override
    public double getLoad() {
        if (loadEma == null) {
            throw new IllegalStateException("Worker load calculation is not enabled");
        }
        return loadEma.getEma();
    }

    /**
     * Both buffer-full callbacks are invoked from whichever thread is producing into the session ring buffers, not
     * from this worker's thread, and they race with the {@code put}/{@code remove} the migration does: a session being
     * handed over to another worker has no state here between the detach and the attach. Missing the notification is
     * harmless, it only feeds a statistic, whereas dereferencing the missing entry throws inside the producer's
     * blocking offer and kills the writing thread mid-send.
     */
    private void onIOSessionTasksBufferFull(IOSession ioSession) {
        RingBufferStates states = ioSessionRingBufferStates.get(ioSession);
        if (states != null) {
            states.getTasksBufferFull().set(true);
        }
    }

    private void onIOSessionWritesBufferFull(IOSession ioSession) {
        RingBufferStates states = ioSessionRingBufferStates.get(ioSession);
        if (states != null) {
            states.getWritesBufferFull().set(true);
        }
    }

    void toggleIoWorkerStats(boolean enable) {
        if (enable && enabledStatistics || !enable && !enabledStatistics) {
            // nothing to do
            return;
        }
        enabledStatistics = enable;
        // enable manually stats only if load EMA is not calculated, because iof calculated it cannot be unset
        if (enabledStatistics && loadEma == null) {
            resetStatsCounters();
            IOStatsScheduler.getInstance().register(statsTask, ioWorkerStatistics.getStatsResolution());
        } else if (loadEma != null) {
            IOStatsScheduler.getInstance().unregister(statsTask);
        }
        synchronized (registeredSessions) {
            registeredSessions.forEach(s -> s.onIOWorkerStatsEnabled(enabledStatistics));
        }
    }

    private void resetStatsCounters() {
        acceptCpuTimeInNanos = connectCpuTimeInNanos = selectedKeysCounter = 0;
    }

    @Override
    public IOWorkerImpl start() throws StartStopException {
        if (running.getAndSet(true)) {
            return this;
        }
        try {
            if (ioWorkersGroupSettings.isOptimizedSelector()) {
                SelectorOptimizer.SelectorPair st = SelectorOptimizer.openSelector(ioWorkersGroupSettings.getSelectorProvider(), this);
                selector = st.getSelector();
                optimizedSelector = st.getOptimizedSelector();
            } else {
                selector = ioWorkersGroupSettings.getSelectorProvider().openSelector();
                optimizedSelector = selector;
            }
            this.selectorWakeupIfNeeded = selectStrategy.requireSelectorWakeup() ? selector::wakeup : null;
        } catch (IOException ex) {
            throw new StartStopException(ex);
        }
        ioWorkerThread.start();
        if (enabledStatistics) {
            resetStatsCounters();
            IOStatsScheduler.getInstance().register(statsTask, ioWorkerStatistics.getStatsResolution());
        }
        return this;
    }

    Class<? extends Thread> getIOWorkerThreadsClass() {
        return ioWorkerThread.getClass();
    }

    @Override
    public boolean isStarted() {
        return running.get();
    }

    boolean isThreadAlive() {
        return ioWorkerThread.isAlive();
    }

    /**
     * Returns only once this worker's thread is dead. The stop deadline param has no effect
     */
    @Override
    public IOWorkerImpl stop(org.lolaf.ringos.Deadline stopDeadline) {
        if (!running.getAndSet(false)) {
            return this;
        }
        if (enabledStatistics) {
            IOStatsScheduler.getInstance().unregister(statsTask);
        }
        if (selectorWakeupIfNeeded != null) {
            // a strategy that parks in select() would otherwise only notice the stop on the next readiness event
            selectorWakeupIfNeeded.run();
        }
        awaitIOWorkerThreadDeath();
        // with the thread dead, a session that could not hand its disconnection over to it can have its buffers back
        synchronized (registeredSessions) {
            registeredSessions.forEach(IOSessionImpl::releasePooledBuffers);
        }
        registeredSessions.clear();
        try {
            selector.close();
        } catch (IOException e) {
            log.error("Failed to close selector", e);
        }
        selector = null;
        return this;
    }

    private void awaitIOWorkerThreadDeath() {
        if (isThreadAlive()) {
            if (Thread.currentThread() == ioWorkerThread) {
                log.error("IO worker thread {} is stopping itself from within its own thread, abnormal", getName());
                return;
            }
            try {
                ioWorkerThread.interrupt();
                ioWorkerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public int getRegisteredSessionsCount() {
        return registeredSessions.size();
    }

    @Override
    public List<IOSession> getRegisteredSessions() {
        synchronized (registeredSessions) {
            return List.copyOf(registeredSessions);
        }
    }

    @Override
    public CompletableFuture<Void> migrateIOSession(IOSession session, IOWorker target) {
        if (!getRegisteredSessions().contains(session)) {
            return CompletableFuture.failedFuture(new IllegalStateException("Session " + session + " is not registered"));
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        submitIOWorkerMigration(true, (IOSessionImpl) session, (IOWorkerImpl) target, future);
        return future;
    }

    private void submitIOWorkerMigration(boolean detachSession, IOSessionImpl session, IOWorkerImpl target, CompletableFuture<Void> future) {
        pendingMigrations.offerBlocking(IOWorkerMigrationCommand::translate, detachSession, target, session, future, new BackoffIdleStrategy());
        hasPendingMigrations = true;
        if (selectorWakeupIfNeeded != null) {
            selectorWakeupIfNeeded.run();
        }
    }

    private void onIOWorkerMigrationCommand(IOWorkerMigrationCommand cmd) {
        try {
            if (cmd.detachSession) {
                handleDetach(cmd);
            } else {
                handleAttach(cmd);
            }
        } catch (Exception t) {
            cmd.resultFuture.completeExceptionally(t);
        } finally {
            cmd.clean();
        }
    }

    private void handleDetach(IOWorkerMigrationCommand command) throws IOException {
        IOSessionImpl session = command.session;
        session.detachFromIOWorker();
        // Flush cancelled key from this selector before the channel is re-registered elsewhere.
        selector.selectNow();
        registeredSessions.remove(session);
        ioSessionRingBufferStates.remove(session);
        computeTrackReceiveTime();
        command.target.submitIOWorkerMigration(false, session, null, command.resultFuture);
    }

    private void handleAttach(IOWorkerMigrationCommand command) throws IOException {
        IOSessionImpl session = command.session;
        session.attachToIOWorker(selector, selectStrategy, this::onSessionStopped, this::onIOSessionTasksBufferFull,
                this::onIOSessionWritesBufferFull, this.ioWorkerThread, this);
        registeredSessions.add(session);
        ioSessionRingBufferStates.put(session, new RingBufferStates());
        computeTrackReceiveTime();
        log.info("IOSession {} has been attached to IOWorker {}", session.getId(), this.getName());
        command.resultFuture.complete(null);
    }

    private void runLoop() {
        // on the IO thread and before the first select: an idle strategy that narrows the OS timer slack sets it on
        // whichever thread calls it, so this is the only place that can do it for the thread that will park
        selectStrategy.assignToThread(Thread.currentThread());
        log.info("IO Worker thread {} started", Thread.currentThread().getName());
        while (running.get()) {
            try {
                selectedKeysCounter += selectStrategy.select(optimizedSelector, selectKeyConsumer);
            } catch (IOException ex) {
                log.error("CRITICAL: IOException occurred in IO thread {}", ioWorkerThread.getName(), ex);
            }
            if (hasPendingMigrations) {
                hasPendingMigrations = false;
                while (pendingMigrations.poll(this::onIOWorkerMigrationCommand)) {
                    // drain
                }
            }
            if (trackReceiveTime) {
                localReceiveTime = ZERO;
            }
            if (pendingHandshakesCount > 0) {
                // a peer that connects and then says nothing produces no readiness events, so a handshake left
                // outstanding is only ever noticed by looking. Gated on the count so that a worker carrying no
                // handshake - which is every worker, almost all of the time - pays one field read for it.
                expireTimedOutHandshakes();
            }
        }
        // must do a last select now to cleanly close all incoming socket and acceptors bound to the selector
        try {
            optimizedSelector.selectNow(selectKeyConsumer);
        } catch (IOException e) {
            log.info("Failed to call Selector selectNow to finalize IOWorker", e);
        }
        log.info("IO Worker thread {} stopped", Thread.currentThread().getName());
    }

    private void runStats() {
        synchronized (registeredSessions) {
            AtomicLong totalCpuTimeTakenInNanos = new AtomicLong();
            registeredSessions.forEach(s -> {
                long statsTime = System.currentTimeMillis();
                statsTime -= statsTime % 1000;
                long localAcceptCpuTime = acceptCpuTimeInNanos;
                long localConnectCpuTime = connectCpuTimeInNanos;
                int localSelectedKeysCounter = selectedKeysCounter;
                resetStatsCounters();
                ioWorkerStatistics.onStatsCollectionStart(this, statsTime);
                IOSessionImpl.CpuTimeStats transferTo = s.onIOWorkerStats(new IOSessionImpl.CpuTimeStats());
                ioWorkerStatistics.onReadCPUTimeTaken(this, s, transferTo.getReadCpuTimeInNanos());
                ioWorkerStatistics.onWriteCPUTimeTaken(this, s, transferTo.getWriteCpuTimeInNanos());
                ioWorkerStatistics.onTasksCPUTimeTaken(this, s, transferTo.getTasksCpuTimeInNanos());
                ioWorkerStatistics.onAcceptCPUTimeTaken(this, localAcceptCpuTime);
                ioWorkerStatistics.onConnectCPUTimeTaken(this, localConnectCpuTime);
                ioWorkerStatistics.onSelectedKeysCount(this, localSelectedKeysCounter);
                RingBufferStates ioSessionRingBufferState = ioSessionRingBufferStates.get(s);
                if (ioSessionRingBufferState != null) {
                    if (ioSessionRingBufferState.getWritesBufferFull().getAndSet(false)) {
                        ioWorkerStatistics.onSessionWritesRingBufferFull(this, s);
                    }
                    if (ioSessionRingBufferState.getTasksBufferFull().getAndSet(false)) {
                        ioWorkerStatistics.onSessionTasksRingBufferFull(this, s);
                    }
                }
                if (loadEma != null) {
                    totalCpuTimeTakenInNanos.getAndAdd(localAcceptCpuTime
                            + localConnectCpuTime
                            + transferTo.getReadCpuTimeInNanos()
                            + transferTo.getWriteCpuTimeInNanos()
                            + transferTo.getTasksCpuTimeInNanos());
                }
                ioWorkerStatistics.onStatsCollectionEnd(this);
            });
            loadEma.update(TimeUnit.NANOSECONDS.toMicros(totalCpuTimeTakenInNanos.get()));
        }
    }

    private void onSelectionKey(SelectionKey key) {
        if (trackReceiveTime && localReceiveTime == ZERO) {
            localReceiveTime = System.nanoTime();
        }
        if (!key.isValid()) {
            log.info("Skipping invalid key {}", key);
            return;
        }
        try {
            processSelectionKey(key);
        } catch (CancelledKeyException ex) {
            // can happen when stopping the session
            if (key.attachment() instanceof IOSessionImpl) {
                IOSessionImpl ioSession = ((IOSessionImpl) key.attachment());
                if (ioSession.isStarted()) {
                    log.error("Key is cancelled with still started session");
                }
            }
            log.debug("Key is cancelled");
        } catch (Exception ex) {
            if (key.attachment() instanceof IOSessionImpl) {
                IOSessionImpl ioSession = (IOSessionImpl) key.attachment();
                // clean socket close will trigger an EOFException
                if (!(ex instanceof EOFException)) {
                    ioSession.getIoEventsListener().onError(ioSession, ex);
                }
                // important give no deadline or it will try to flush messages still in buffer
                // however here we have a terminal an IO error
                ioSession.stop(org.lolaf.ringos.Deadline.immediate());
            } else {
                log.error("Unexpected error occurred", ex);
            }
        }
    }

    private void processSelectionKey(SelectionKey key) throws IOException {
        if (key.isReadable()) {
            ((IOSessionImpl) key.attachment()).onOperationRead(localReceiveTime);
        } else if (key.isWritable()) {
            ((IOSessionImpl) key.attachment()).onOperationWrite();
        } else if (key.isConnectable()) {
            connectIOSession(key);
        } else if (key.isAcceptable()) {
            acceptIOSession(key);
        }
    }

    private void connectIOSession(SelectionKey key) throws IOException {
        long startTime = System.nanoTime();
        SocketChannel socket = (SocketChannel) key.channel();
        BaseBuilder baseBuilder = (BaseBuilder) key.attachment();
        try {
            if (log.isDebugEnabled()) {
                log.debug("IOSession connect {} {}:{}:{}", socket.getRemoteAddress(), socket.isConnected(), socket.isOpen(), socket.isConnectionPending());
            }
            if (socket.isConnectionPending()) {
                socket.finishConnect();
                register(true, socket, baseBuilder);
            }
        } catch (ConnectException e) {
            // happens when calling finishConnect() and a client socket connection failed to establish
            baseBuilder.getIoEventsListener().onDisconnected(null);
        }
        connectCpuTimeInNanos += System.nanoTime() - startTime;
    }

    private void acceptIOSession(SelectionKey key) throws IOException {
        long startTime = System.nanoTime();
        ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
        SocketChannel socket = serverSocketChannel.accept();
        ServerBuilder serverBuilder = (ServerBuilder) key.attachment();
        IOWorker target = serverBuilder.getIoWorkersGroup().getNext(socket);
        // register on the accepting worker, then hand the session off through the existing detach/attach migration
        // if the session group routes it elsewhere: a selector can only be registered from the thread owning it
        IOSessionImpl ioSession = registerInternal(false, socket, serverBuilder);
        if (ioSession != null && target != this) {
            migrateIOSession(ioSession, target);
        }
        acceptCpuTimeInNanos += System.nanoTime() - startTime;
    }

    @Override
    public void register(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder) throws IOException {
        registerInternal(clientSession, socket, baseBuilder);
    }

    /**
     * Closes any secure session whose handshake has run out of time. Kept off the run loop's normal path by
     * {@link #pendingHandshakesCount}, and iterating under the monitor as every other traversal of
     * {@link #registeredSessions} does - it is a synchronized set, which makes single operations atomic and leaves
     * iteration to the caller.
     */
    private void expireTimedOutHandshakes() {
        long nowInNanos = System.nanoTime();
        List<SecureIOSession> handshaking = null;
        synchronized (registeredSessions) {
            for (IOSessionImpl session : registeredSessions) {
                if (session instanceof SecureIOSession && ((SecureIOSession) session).isHandshaking()) {
                    if (handshaking == null) {
                        handshaking = new ArrayList<>();
                    }
                    handshaking.add((SecureIOSession) session);
                }
            }
        }
        // outside the monitor: expiring one closes it, which removes it from registeredSessions
        pendingHandshakesCount = handshaking == null ? 0 : handshaking.size();
        if (handshaking != null) {
            handshaking.forEach(session -> session.expireHandshakeIfTimedOut(nowInNanos));
        }
    }

    /**
     * Tells the run loop that a handshake is outstanding on this worker, so that it starts looking for expired ones.
     * Only ever raised here and recomputed by {@link #expireTimedOutHandshakes()}.
     */
    void onHandshakeStarted() {
        pendingHandshakesCount++;
    }

    private IOSessionImpl registerInternal(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder) throws IOException {
        log.info("Registering socket {} connected to {}", socket, name);
        socket.configureBlocking(false);
        IOSessionImpl ioSession = createIOSession(clientSession, socket, baseBuilder);
        if (enabledStatistics) {
            ioSession.onIOWorkerStatsEnabled(true);
        }
        try {
            if (ioSession.start(selector, selectStrategy, this::onSessionStopped, this::onIOSessionTasksBufferFull,
                    this::onIOSessionWritesBufferFull, ioWorkerThread, this)) {
                registeredSessions.add(ioSession);
                ioSessionRingBufferStates.put(ioSession, new RingBufferStates());
                computeTrackReceiveTime();
                if (ioSession instanceof SecureIOSession && ((SecureIOSession) ioSession).isHandshaking()) {
                    onHandshakeStarted();
                }
                return ioSession;
            } else {
                ioSession.getIoEventsListener().onSessionRejected(ioSession);
                ioSession.stop(org.lolaf.ringos.Deadline.immediate());
            }
        } catch (IOException ex) {
            if (!(ex instanceof SSLHandshakeException)) {
                ioSession.getIoEventsListener().onError(ioSession, ex);
            }
            ioSession.stop(org.lolaf.ringos.Deadline.immediate());
        }
        return null;
    }

    private IOSessionImpl createIOSession(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder) {
        RemoteSessionsFilter remoteSessionsFilter = (s, cert) -> true;
        SSLSettings sslSetting;
        IOBufferPool sharedIOBufferPool = null;
        if (baseBuilder instanceof ServerBuilder) {
            ServerBuilder serverBuilder = ((ServerBuilder) baseBuilder);
            sharedIOBufferPool = SharedIOBufferPoolInstanceRegistry.getInstance().getPool(baseBuilder.getId());
            remoteSessionsFilter = serverBuilder.getRemoteSessionsFilter();
            sslSetting = serverBuilder.getServerSSLSettings();
        } else {
            sslSetting = ((ClientBuilder) baseBuilder).getSSLSettings();
        }
        IOSettings ioSettingsForSession = baseBuilder.getIoEventsListener().getIOSettings((InetSocketAddress) socket.socket().getRemoteSocketAddress(), baseBuilder.getIoSettings());
        if (ioSettingsForSession == null) {
            ioSettingsForSession = baseBuilder.getIoSettings();
        }
        return sslSetting == null
                ? new IOSessionImpl(clientSession, socket, baseBuilder, ioSettingsForSession, remoteSessionsFilter, sharedIOBufferPool, ioWorkersGroupSettings)
                : new SecureIOSession(clientSession, socket, baseBuilder, ioSettingsForSession, remoteSessionsFilter, sharedIOBufferPool, ioWorkersGroupSettings);
    }

    private void onSessionStopped(IOSession ioSession) {
        registeredSessions.remove(ioSession);
        ioSessionRingBufferStates.remove(ioSession);
        computeTrackReceiveTime();
    }

    private void computeTrackReceiveTime() {
        synchronized (registeredSessions) {
            trackReceiveTime = registeredSessions.stream().anyMatch(IOSessionImpl::isTrackReceiveTime);
        }
    }

    @Override
    public String toString() {
        return name + ", sessionsCount=" + registeredSessions.size();
    }

    @Value
    private static class RingBufferStates {
        AtomicBoolean writesBufferFull = new AtomicBoolean();
        AtomicBoolean tasksBufferFull = new AtomicBoolean();
    }

    private static final class IOWorkerMigrationCommand {
        private boolean detachSession;
        private IOWorkerImpl target;
        private IOSessionImpl session;
        private CompletableFuture<Void> resultFuture;

        void translate(boolean detachSession, IOWorkerImpl target, IOSessionImpl session, CompletableFuture<Void> resultFuture) {
            this.detachSession = detachSession;
            this.target = target;
            this.session = session;
            this.resultFuture = resultFuture;
        }

        void clean() {
            session = null;
            target = null;
            resultFuture = null;
        }
    }
}