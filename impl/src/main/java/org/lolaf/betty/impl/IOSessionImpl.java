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

import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.BaseBuilder;
import org.lolaf.betty.api.RemoteSessionsFilter;
import org.lolaf.betty.api.io.*;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.stats.IOStats;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BackoffIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.rb.RingBuffer;
import org.lolaf.ringos.rb.RingBufferFactory;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;

@Slf4j
@ToString(onlyExplicitlyIncluded = true)
class IOSessionImpl implements IOSession {

    private static final long DISCONNECT_ON_IO_THREAD_MIN_WAIT_IN_MILLIS = 100L;
    private static final BiConsumer<Runnable, Exception> DEFAULT_TASK_CALLBACK = (t, e) -> {
        if (e != null) {
            log.error("Failed to execute task", e);
        }
    };
    private final RingBuffer<IOThreadRequest> ioThreadRequests;
    @Getter(AccessLevel.PACKAGE)
    private final SocketChannel socket;
    @Getter
    private final IOEventsListener ioEventsListener;
    @Getter
    private final InetSocketAddress socketAddress;
    @Getter
    private final IOBufferPool ioBufferPool;
    private final boolean sharedIOBufferPool;
    @Getter(AccessLevel.PACKAGE)
    private final RemoteSessionsFilter remoteSessionsFilter;
    private final WriteWatermarkStateTracker writeWatermarkState;
    private final int maxBytesCountPerWriteCycle;
    private final boolean orderedWrites;
    @Getter(AccessLevel.PACKAGE)
    private final AtomicBoolean started;
    private final IntFunction<ByteBuffer> readBufferAllocator;
    @Getter(AccessLevel.PACKAGE)
    private final boolean trackReceiveTime;
    private final Set<IOSessionTag> tags;
    private final IOThreadRequest localIOThreadRequest;
    private final IOThreadRequest pendingWrite;
    private final Consumer<IOThreadRequest> localIOThreadWorkRequestConsumer;
    private final IOStats ioStats;
    private final CpuTimeStats cpuTimeStats;
    private boolean insideWriteCycle;
    private Consumer<IOSession> sessionStoppedConsumer;
    private Thread ioWorkerThread;
    private IdleStrategy ioThreadSocketWriteRequestsIdleStrategy;
    private IdleStrategy ioThreadTaskRequestsIdleStrategy;
    @Getter(AccessLevel.PACKAGE)
    private IOStats activeIOStats;
    @Getter
    @Setter
    @ToString.Include
    private String id;
    @Getter(AccessLevel.PACKAGE)
    private volatile SelectionKey selectionKey;
    @Getter(AccessLevel.PACKAGE)
    @Setter(AccessLevel.PACKAGE)
    private ByteBuffer readBuffer;
    private Object attachment;
    private IOWriter ioWriter;
    private IOSessionTag[] tagsArray;
    private Runnable wakeupSelectorIfNeeded;
    private boolean ioWorkerStatsEnabled;
    private volatile boolean processingReadOperation;
    private boolean paused;
    @Getter(AccessLevel.PACKAGE)
    private IOWorker ownerWorker;
    private boolean assignedToIOWorker;

    public IOSessionImpl(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder, IOSettings ioSettings, RemoteSessionsFilter remoteSessionsFilter,
                         IOBufferPool sharedIOBufferPool, IOWorkersGroupSettings ioWorkersGroupSettings) {
        this.socket = socket;
        this.ioEventsListener = baseBuilder.getIoEventsListener();
        this.socketAddress = (InetSocketAddress) socket.socket().getRemoteSocketAddress();
        IOBufferPoolSettings.MultiThreadingAccessMode poolMultiThreadingMode = ioSettings.isMultiThreadedWriteAPICalls()
                ? IOBufferPoolSettings.MultiThreadingAccessMode.MULTIPLE_BORROWER_THREADS
                : IOBufferPoolSettings.MultiThreadingAccessMode.ONE_BORROWER_THREAD;
        ioSettings = ioSettings.toBuilder().writeIoBufferPoolSettings(ioSettings.getWriteIoBufferPoolSettings().toBuilder()
                .multiThreadingAccessMode(poolMultiThreadingMode).build()).build();
        this.pendingWrite = new IOThreadRequest(0);
        this.orderedWrites = ioSettings.isOrderedWrites();
        // ordered writes make the IO thread queue its own messages, and so a producer of the ring on top of whichever
        // application threads write to the session
        this.ioThreadRequests = ioSettings.isMultiThreadedWriteAPICalls() || orderedWrites
                ? RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_MULTI_PRODUCER, ioSettings.getTasksRingBufferSize(), IOThreadRequest::new)
                : RingBufferFactory.build(RingBufferFactory.AccessType.SINGLE_CONSUMER_SINGLE_PRODUCER, ioSettings.getTasksRingBufferSize(), IOThreadRequest::new);
        this.ioBufferPool = sharedIOBufferPool != null ? sharedIOBufferPool : ioSettings.getWriteIoBufferPoolSettings().newInstance();
        this.sharedIOBufferPool = sharedIOBufferPool != null;
        this.readBufferAllocator = ioSettings.isReadDirectBuffer() ? ByteBuffer::allocateDirect : ByteBuffer::allocate;
        this.readBuffer = readBufferAllocator.apply(ioSettings.getReadBufferSize());
        this.started = new AtomicBoolean(false);
        this.localIOThreadRequest = new IOThreadRequest(0);
        this.localIOThreadWorkRequestConsumer = localIOThreadRequest::transferFromPoll;
        ioSettings.getSocketOptions().forEach((so, val) -> {
            try {
                socket.setOption(so, val);
            } catch (IOException e) {
                log.error("Failed to set socket option {}", so, e);
            }
        });
        this.id = clientSession ? baseBuilder.getId() : baseBuilder.getId() + ":" + socketAddress;
        this.remoteSessionsFilter = remoteSessionsFilter;
        this.writeWatermarkState = ioSettings.getWriteHighWatermark() > 0 && ioSettings.getWriteLowWatermark() > 0
                ? new ActiveWriteWatermarkStateTracker(ioSettings.getWriteHighWatermark(), ioSettings.getWriteLowWatermark(), new AtomicInteger(), this, ioEventsListener)
                : InactiveWriteWatermarkStateTracker.INSTANCE;
        this.ioWriter = new ActiveIOWriter();
        IOStats localIOStats = baseBuilder.getIoStatsProvider().get(this);
        if (localIOStats instanceof IOStats.VoidStats) {
            localIOStats = ensureCorrectVoidIOStatsInCaseOfLoadCalculation(ioWorkersGroupSettings, (IOStats.VoidStats) localIOStats);
        } else {
            localIOStats = new FailSafeIOStats(localIOStats);
        }
        this.ioStats = localIOStats;
        this.activeIOStats = ioStats.enabledByDefault() ? ioStats : ensureCorrectVoidIOStatsInCaseOfLoadCalculation(ioWorkersGroupSettings, IOStats.VoidStats.getInstance());

        this.maxBytesCountPerWriteCycle = ioSettings.getMaxBytesCountPerWriteCycle();
        this.trackReceiveTime = ioSettings.isTrackReceiveTime();
        this.tags = new HashSet<>();
        this.tagsArray = new IOSessionTag[0];
        this.cpuTimeStats = new CpuTimeStats();
    }

    private static IOStats ensureCorrectVoidIOStatsInCaseOfLoadCalculation(IOWorkersGroupSettings ioWorkersGroupSettings, IOStats.VoidStats voidStats) {
        if (ioWorkersGroupSettings.getIoWorkerLoadBalancer().requiresIOWorkersLoadComputation()) {
            // important if we want to be able to calculate workers we must make sure that in case of a void stats listener
            // we still return the System.nanoTime() when asking for the time, or we won't be able to calculate anything
            // for the work load
            return new IOStats() {

                @Override
                public boolean enabledByDefault() {
                    return false;
                }

                @Override
                public long getTimeInNanos(Operation operation) {
                    return System.nanoTime();
                }
            };
        }
        return voidStats;
    }

    private static void discardRemaining(ByteBuffer bufferOut) {
        if (bufferOut != null) {
            bufferOut.position(bufferOut.limit());
        }
    }

    private BackoffIdleStrategy getIdleStrategy(Consumer<IOSession> ringBufferFullConsumer) {
        return new BackoffIdleStrategy() {

            @Override
            public void reset() {
                super.reset();
                ringBufferFullConsumer.accept(IOSessionImpl.this);
            }
        };
    }

    void toggleIoStats(boolean enable) {
        this.activeIOStats = enable ? ioStats : IOStats.VoidStats.getInstance();
    }

    void toggleIOBufferPoolStats(boolean enable) {
        if (!sharedIOBufferPool) {
            ioBufferPool.toggleIOBufferPoolStats(enable);
        }
    }

    void onIOWorkerStatsEnabled(boolean enabled) {
        ioWorkerStatsEnabled = enabled;
        cpuTimeStats.reset();
    }

    CpuTimeStats onIOWorkerStats(CpuTimeStats transferTo) {
        cpuTimeStats.transfer(transferTo);
        cpuTimeStats.reset();
        return transferTo;
    }

    @Override
    public void disableStats() {
        activeIOStats = IOStats.VoidStats.getInstance();
    }

    @Override
    public boolean isIOThread(Thread thread) {
        return thread == ioWorkerThread;
    }

    @Override
    public boolean isWithinIOThread() {
        return Thread.currentThread() == ioWorkerThread;
    }

    @Override
    public void pause(Deadline flushAllReadsAndWritesDeadline) {
        paused = true;
        if (!flushAllReadsAndWritesDeadline.isImmediate()) {
            waitForAllMessagesRead(flushAllReadsAndWritesDeadline.fromRemainingTime(0.5));
            waitForAllMessagesSent(flushAllReadsAndWritesDeadline);
        }
        processTaskWithCountdownLatch(countDownLatch -> {
            selectionKey.interestOpsAnd(~SelectionKey.OP_READ);
            selectionKey.interestOpsAnd(~SelectionKey.OP_WRITE);
            countDownLatch.countDown();
        });
    }

    @Override
    public void resume() {
        processTaskWithCountdownLatch(countDownLatch -> {
            selectionKey.interestOpsOr(SelectionKey.OP_READ);
            // since OP WRITE is selected when creating the task, the ioThreadRequests will be likely empty
            // and write message processed before this one is done
            if (!ioThreadRequests.isEmpty()) {
                selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
            }
            paused = false;
            countDownLatch.countDown();
        });
    }

    private void processTaskWithCountdownLatch(Consumer<CountDownLatch> ct) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        processTask(() -> ct.accept(countDownLatch));
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    boolean isConnectionAllowed() {
        return remoteSessionsFilter.allow(socketAddress, null);
    }

    private void nothingToDo() {
        // well nothing to do
    }

    boolean start(Selector selector, SelectStrategy selectStrategy, Consumer<IOSession> sessionStoppedConsumer,
                  Consumer<IOSession> tasksRingBufferFullConsumer, Consumer<IOSession> writesRingBufferFullConsumer,
                  Thread ioWorkerThread, IOWorker ioWorker) throws IOException {
        started.set(true);
        if (!isConnectionAllowed()) {
            return false;
        }
        prepareForIO(selector, selectStrategy, sessionStoppedConsumer, tasksRingBufferFullConsumer,
                writesRingBufferFullConsumer, ioWorkerThread, ioWorker);
        notifyConnected();
        return true;
    }

    /**
     * Everything {@link #start} does to make the session able to do I/O, short of declaring it usable: the selection
     * key exists once this returns, so the session is receiving readiness events.
     */
    void prepareForIO(Selector selector, SelectStrategy selectStrategy, Consumer<IOSession> sessionStoppedConsumer,
                      Consumer<IOSession> tasksRingBufferFullConsumer, Consumer<IOSession> writesRingBufferFullConsumer,
                      Thread ioWorkerThread, IOWorker ioWorker) throws IOException {
        this.sessionStoppedConsumer = sessionStoppedConsumer;
        this.ioThreadTaskRequestsIdleStrategy = getIdleStrategy(tasksRingBufferFullConsumer);
        this.ioThreadSocketWriteRequestsIdleStrategy = getIdleStrategy(writesRingBufferFullConsumer);
        if (!sharedIOBufferPool) {
            ioBufferPool.start(this);
        }
        attachToIOWorker(selector, selectStrategy, sessionStoppedConsumer, tasksRingBufferFullConsumer, writesRingBufferFullConsumer, ioWorkerThread, ioWorker);
    }

    void notifyConnected() {
        this.ioEventsListener.onConnected(this);
        this.activeIOStats.onSessionOpened(this);
    }

    void setInterestOps(int interestOps) {
        selectionKey.interestOps(interestOps);
    }

    void detachFromIOWorker() {
        assignedToIOWorker = false;
        if (hasActiveSelectionKey()) {
            selectionKey.cancel();
        }
    }

    void attachToIOWorker(Selector selector, SelectStrategy selectStrategy, Consumer<IOSession> sessionStoppedConsumer,
                          Consumer<IOSession> tasksRingBufferFullConsumer, Consumer<IOSession> writesRingBufferFullConsumer,
                          Thread ioWorkerThread, IOWorker ioWorker) throws IOException {
        this.ownerWorker = ioWorker;
        this.sessionStoppedConsumer = sessionStoppedConsumer;
        this.ioThreadTaskRequestsIdleStrategy = getIdleStrategy(tasksRingBufferFullConsumer);
        this.ioThreadSocketWriteRequestsIdleStrategy = getIdleStrategy(writesRingBufferFullConsumer);
        this.ioWorkerThread = ioWorkerThread;
        this.wakeupSelectorIfNeeded = selectStrategy.requireSelectorWakeup() ? selector::wakeup : this::nothingToDo;
        this.selectionKey = socket.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, this);
        this.assignedToIOWorker = true;
    }

    @Override
    public NetworkChannel getNetworkChannel() {
        return socket;
    }

    @Override
    public void addTag(IOSessionTag tag) {
        tags.add(tag);
        generateTagsArray();
    }

    @Override
    public void removeTag(IOSessionTag tag) {
        tags.remove(tag);
        generateTagsArray();
    }

    private void generateTagsArray() {
        tagsArray = tags.toArray(tags.toArray(new IOSessionTag[0]));
    }

    @Override
    public boolean matchesTag(IOSessionTag tag) {
        IOSessionTag[] localTags = tagsArray;
        for (IOSessionTag localTag : localTags) {
            if (localTag.equals(tag)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean matchesTags(IOSessionTag... tags) {
        IOSessionTag[] localTags = tagsArray;
        if (localTags.length < tags.length) {
            return false;
        }
        for (IOSessionTag tag : tags) {
            boolean match = false;
            for (IOSessionTag localTag : localTags) {
                if (localTag.equals(tag)) {
                    match = true;
                    break;
                }
            }
            if (!match) {
                return false;
            }
        }
        return true;
    }

    @Override
    public <T> T getAttachment() {
        return (T) attachment;
    }

    @Override
    public void setAttachment(Object attachment) {
        this.attachment = attachment;
    }

    @Override
    public void stop(Deadline flushOutgoingMessagesDeadline) {
        if (started.getAndSet(false)) {
            // immediately stop reads
            if (hasActiveSelectionKey()) {
                selectionKey.interestOpsAnd(~SelectionKey.OP_READ);
            }
            ioEventsListener.onShutdown(this);
            ioWriter = new InactiveIOWriter();
            if (!flushOutgoingMessagesDeadline.isImmediate() && !isWithinIOThread()) {
                waitForAllMessagesRead(flushOutgoingMessagesDeadline.fromRemainingTime(0.5));
                waitForAllMessagesSent(flushOutgoingMessagesDeadline);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
                // unfortunately we have to wait for OS to flush last message IO even if queue is empty
            }
            disconnectOnIOThread(flushOutgoingMessagesDeadline);
            if (sessionStoppedConsumer != null) {
                sessionStoppedConsumer.accept(this);
            }
            tags.clear();
            generateTagsArray();
        }
    }

    protected boolean hasActiveSelectionKey() {
        return selectionKey != null && selectionKey.isValid();
    }

    /**
     * Runs {@link #disconnect()} on the thread that owns this session.
     */
    private void disconnectOnIOThread(Deadline deadline) {
        if (isWithinIOThread() || !hasActiveSelectionKey()
                || ioWorkerThread == null || !ioWorkerThread.isAlive()) {
            disconnect();
            return;
        }
        CountDownLatch disconnected = new CountDownLatch(1);
        AtomicBoolean ranOnIOThread = new AtomicBoolean();
        // the latch also counts down when the task is rejected, so it says the handover is over and not that it ran
        boolean answered = false;
        try {
            processTask(() -> {
                try {
                    disconnect();
                    ranOnIOThread.set(true);
                } finally {
                    disconnected.countDown();
                }
            }, (task, error) -> disconnected.countDown());
            // an immediate deadline still gets a moment here: this is the difference between a clean disconnection
            // and one leaving buffers behind, not a flush the caller asked to skip
            answered = disconnected.await(Math.max(DISCONNECT_ON_IO_THREAD_MIN_WAIT_IN_MILLIS, deadline.getRemainingTime().toMillis()),
                    TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.info("Failed to hand the disconnection of IOSession {} to its IO thread", id, e);
        }
        if (!ranOnIOThread.get()) {
            log.warn("IOSession {} was {} by its IO thread, closing the socket from {} and leaving its pooled buffers"
                            + " to be released when the IO worker stops", id,
                    answered ? "refused" : "not disconnected within " + deadline, Thread.currentThread().getName());
            disconnect(false);
        }
    }

    @Override
    public boolean isStarted() {
        return started.get();
    }

    /**
     * Grows a <b>flipped</b> read buffer by {@code increasePercentage}, never past {@code maxSize}, keeping what it
     * still holds. {@code maxSize} is a ceiling: the result may well be smaller than it, and smaller than the caller
     * hoped for.
     */
    ByteBuffer resizeReadBuffer(ByteBuffer buffer, double increasePercentage, int maxSize) {
        int newSize = buffer.capacity() + (int) (buffer.capacity() * increasePercentage);
        newSize = Math.min(maxSize, newSize);
        ByteBuffer newReadBuffer = readBufferAllocator.apply(newSize);
        if (buffer.hasRemaining()) {
            newReadBuffer.put(buffer);
        }
        return newReadBuffer;
    }

    /**
     * Returns a <b>write mode</b> read buffer with room for {@code requiredFreeSpace} more bytes, keeping whatever
     * has already been written into the current one, or that same buffer when it has the room already.
     */
    ByteBuffer growReadBufferFor(ByteBuffer buffer, int requiredFreeSpace) {
        if (buffer.remaining() >= requiredFreeSpace) {
            return buffer;
        }
        ByteBuffer grown = readBufferAllocator.apply(buffer.position() + requiredFreeSpace);
        if (buffer.position() > 0) {
            grown.put(buffer.flip());
        }
        return grown;
    }

    @Override
    public ByteBuffer borrow(int capacity) {
        return ioWriter.borrow(capacity);
    }

    @Override
    public void unborrow(ByteBuffer borrowed) {
        ioWriter.unborrow(borrowed);
    }

    @Override
    public ByteBuffer allocate(int capacity) {
        return ioWriter.allocate(capacity);
    }

    @Override
    public <C> CompletableFuture<C> send(ByteBuffer message, C messageSendingContext, boolean ioBufferPoolByteBuffer) {
        return ioWriter.send(message, messageSendingContext, ioBufferPoolByteBuffer);
    }

    @Override
    public <C> void send(ByteBuffer message, C messageSendingContext, MessageSentCallback<C> messageSentCallback, boolean ioBufferPoolByteBuffer) {
        ioWriter.send(message, messageSendingContext, messageSentCallback, ioBufferPoolByteBuffer);
    }

    @Override
    public <C> void send(ByteBufferBuilder byteBufferBuilder, C messageSendingContext, MessageSentCallback<C> messageSentCallback) {
        ioWriter.send(byteBufferBuilder, messageSendingContext, messageSentCallback);
    }

    @Override
    public void send(byte[] message) {
        ioWriter.send(message);
    }

    @Override
    public void send(ByteBuffer message, boolean ioBufferPoolByteBuffer) {
        ioWriter.send(message, ioBufferPoolByteBuffer);
    }

    @Override
    public boolean waitForAllMessagesSent(Deadline maxWaitTime) {
        return maxWaitTime.waitAsLongAs(this::hasUnsentMessages);
    }

    @Override
    public boolean waitForAllMessagesRead(Deadline maxWaitTime) {
        return maxWaitTime.waitAsLongAs(() -> processingReadOperation);
    }

    @Override
    public void processTask(Runnable task) {
        processTask(task, DEFAULT_TASK_CALLBACK);
    }

    @Override
    public void processTask(Runnable task, BiConsumer<Runnable, Exception> callback) {
        if (Thread.currentThread() == ioWorkerThread) {
            executeTask(task, callback);
            return;
        }
        ioThreadRequests.offerBlocking(this::translateTask, task, callback, ioThreadTaskRequestsIdleStrategy);
        try {
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
            wakeupSelectorIfNeeded.run();
        } catch (CancelledKeyException ex) {
            if (!assignedToIOWorker) {
                // we may have detached the session to migrate to a new io thread
                return;
            }
            callback.accept(task, new EOFException("Cannot process IOTask on disconnected session " + id));
        }
    }

    private void translateTask(IOThreadRequest wr, Runnable task, BiConsumer<Runnable, Exception> callback) {
        wr.translate(task, callback);
    }

    final void onOperationRead(long localReceiveTimeInNanos) throws IOException {
        processingReadOperation = true;
        try {
            onOperationReadInternal(localReceiveTimeInNanos);

        } finally {
            processingReadOperation = false;
        }
    }

    void onOperationReadInternal(long localReceiveTimeInNanos) throws IOException {
        if (ioWorkerStatsEnabled) {
            long localReadStartTime = activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_READ);
            processRead(localReceiveTimeInNanos, localReadStartTime);
            cpuTimeStats.readCpuTimeInNanos += activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_READ) - localReadStartTime;
            return;
        }
        processRead(localReceiveTimeInNanos, 0L);
    }

    private void processRead(long localReceiveTimeInNanos, long localReadStartTime) throws IOException {
        int readenBytes = readFromSocket(readBuffer);
        activeIOStats.onSocketRead(this, readenBytes, localReadStartTime);
        processIOEventsListenerOnRead(readBuffer.flip(), localReceiveTimeInNanos);
    }

    void processIOEventsListenerOnRead(ByteBuffer readenBuffer, long localReceiveTimeInNanos) {
        long methodCallStartTimeInNanos = activeIOStats.getTimeInNanos(IOStats.Operation.IO_EVENTS_LISTENER_ON_READ);
        ioEventsListener.onRead(this, readenBuffer, localReceiveTimeInNanos);
        activeIOStats.onIOEventsListenerOnReadCall(this, methodCallStartTimeInNanos);
        readenBuffer.compact();
    }

    protected int readFromSocket(ByteBuffer dst) throws IOException {
        int readenBytes = socket.read(dst);
        if (readenBytes < 0) {
            throw new EOFException();
        }
        return readenBytes;
    }

    final void onOperationWrite() throws IOException {
        insideWriteCycle = true;
        try {
            onOperationWriteInternal();
        } finally {
            insideWriteCycle = false;
        }
    }

    void onOperationWriteInternal() throws IOException {
        if (ioWorkerStatsEnabled) {
            processPendingRequestsWithStats(maxBytesCountPerWriteCycle);
            return;
        }
        processPendingRequests(maxBytesCountPerWriteCycle);
    }

    private void processPendingRequests(int maxWritableBytesCount) throws IOException {
        if (pendingWrite.isNotFullyWritten()) {
            maxWritableBytesCount -= resumePendingWrite(0L);
        }
        // nothing may be polled while a message is half out, whoever left it there: a request taken from the ring now
        // would put its bytes in the middle of that one
        while (canContinueProcessingIOThreadRequests(maxWritableBytesCount)
                && ioThreadRequests.poll(localIOThreadWorkRequestConsumer)) {
            if (localIOThreadRequest.isIOWriteOperation()) {
                maxWritableBytesCount -= processIOWriteOperationForRegularBuffer(0L);
            } else if (localIOThreadRequest.isIOWriteOperationWithByteBufferBuilder()) {
                maxWritableBytesCount -= processIOWriteOperationForByteBufferBuilder(0L);
            } else {
                executeTask(localIOThreadRequest.task, localIOThreadRequest.taskCallback);
                localIOThreadRequest.cleanIOThreadTask();
            }
        }
        clearWriteInterestUnlessRequeued();
    }

    private void processPendingRequestsWithStats(int maxWritableBytesCount) throws IOException {
        if (pendingWrite.isNotFullyWritten()) {
            long resumeStartTime = activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE);
            maxWritableBytesCount -= resumePendingWrite(resumeStartTime);
            cpuTimeStats.writeCpuTimeInNanos += activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE) - resumeStartTime;
        }
        while (canContinueProcessingIOThreadRequests(maxWritableBytesCount)
                && ioThreadRequests.poll(localIOThreadWorkRequestConsumer)) {
            if (localIOThreadRequest.isIOWriteOperation()) {
                long startTime = activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE);
                maxWritableBytesCount -= processIOWriteOperationForRegularBuffer(startTime);
                cpuTimeStats.writeCpuTimeInNanos += activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE) - startTime;
            } else if (localIOThreadRequest.isIOWriteOperationWithByteBufferBuilder()) {
                long startTime = activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE);
                maxWritableBytesCount -= processIOWriteOperationForByteBufferBuilder(startTime);
                cpuTimeStats.writeCpuTimeInNanos += activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE) - startTime;
            } else {
                long startTime = activeIOStats.getTimeInNanos(IOStats.Operation.IO_THREAD_TASK);
                executeTask(localIOThreadRequest.task, localIOThreadRequest.taskCallback);
                localIOThreadRequest.cleanIOThreadTask();
                cpuTimeStats.tasksCpuTimeInNanos += activeIOStats.getTimeInNanos(IOStats.Operation.IO_THREAD_TASK) - startTime;
            }
        }
        clearWriteInterestUnlessRequeued();
    }

    private boolean canContinueProcessingIOThreadRequests(int maxWritableBytesCount) {
        return pendingWrite.isFullyWritten() && maxWritableBytesCount >= 0;
    }

    private void clearWriteInterestUnlessRequeued() {
        selectionKey.interestOpsAnd(~SelectionKey.OP_WRITE);
        if (ioThreadRequests.isNotEmpty() || pendingWrite.isNotFullyWritten()) {
            selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
        }
    }

    private int processIOWriteOperationForByteBufferBuilder(long localWriteStartTimeInNanos) throws IOException {
        ByteBufferBuilder bbb = localIOThreadRequest.byteBufferBuilder;
        // built inside the try: a builder that throws is a failed write like any other, and its caller has to be
        // told and its sending context released
        ByteBuffer bufferOut = null;
        try {
            localIOThreadRequest.watermarkEnqueuedBytes = bbb.getEstimatedByteBufferSize();
            bufferOut = bbb.build();
            // from here the built buffer is the write, so a partial one resumes as an ordinary buffer
            localIOThreadRequest.byteBuffer = bufferOut;
            localIOThreadRequest.ioBufferPoolByteBuffer = bbb.isPooledByteBuffer();
            return writeBufferOut(localIOThreadRequest, false, localWriteStartTimeInNanos);
        } catch (IOException ex) {
            safelyProcessCallbackOnIOException(ex,
                    localIOThreadRequest.writeFuture,
                    localIOThreadRequest.messageSentCallback,
                    localIOThreadRequest.messageSendingContext,
                    bufferOut);
            discardRemaining(bufferOut);
            throw ex;
        } finally {
            localIOThreadRequest.byteBufferBuilder = null;
            if (bufferOut != null && bufferOut.hasRemaining()) {
                pendingWrite.transferFromPoll(localIOThreadRequest);
            } else {
                if (bufferOut != null && bbb.isPooledByteBuffer()) {
                    ioBufferPool.returnByteBuffer(bufferOut);
                }
                localIOThreadRequest.cleanRegularIOWriteOperation();
                localIOThreadRequest.cleanByteBufferBuilderIOWriteOperation();
            }
        }
    }

    private int processIOWriteOperationForRegularBuffer(long localWriteStartTime) throws IOException {
        ByteBuffer bufferOut = localIOThreadRequest.byteBuffer;
        try {
            localIOThreadRequest.watermarkEnqueuedBytes = bufferOut.position();
            return writeBufferOut(localIOThreadRequest, false, localWriteStartTime);
        } catch (IOException ex) {
            safelyProcessCallbackOnIOException(ex,
                    localIOThreadRequest.writeFuture,
                    localIOThreadRequest.messageSentCallback,
                    localIOThreadRequest.messageSendingContext,
                    bufferOut);
            discardRemaining(bufferOut);
            throw ex;
        } finally {
            if (bufferOut.hasRemaining()) {
                pendingWrite.transferFromPoll(localIOThreadRequest);
            } else {
                if (localIOThreadRequest.ioBufferPoolByteBuffer) {
                    ioBufferPool.returnByteBuffer(bufferOut);
                }
                localIOThreadRequest.cleanRegularIOWriteOperation();
            }
        }
    }

    private int resumePendingWrite(long localWriteStartTime) throws IOException {
        ByteBuffer bufferOut = pendingWrite.byteBuffer;
        // read before the write: its own callback can hand the slot to another message
        boolean pooledBuffer = pendingWrite.ioBufferPoolByteBuffer;
        try {
            return writeBufferOut(pendingWrite, true, localWriteStartTime);
        } catch (IOException ex) {
            safelyProcessCallbackOnIOException(ex,
                    pendingWrite.writeFuture,
                    pendingWrite.messageSentCallback,
                    pendingWrite.messageSendingContext,
                    bufferOut);
            discardRemaining(bufferOut);
            throw ex;
        } finally {
            if (!bufferOut.hasRemaining()) {
                if (pooledBuffer) {
                    ioBufferPool.returnByteBuffer(bufferOut);
                }
                if (pendingWrite.byteBuffer == bufferOut) {
                    pendingWrite.cleanRegularIOWriteOperation();
                }
            }
        }
    }

    private int writeBufferOut(IOThreadRequest request, boolean resuming, long localWriteStartTime) throws IOException {
        ByteBuffer bufferOut = request.byteBuffer;
        int consumedBefore = resuming ? bufferOut.position() : 0;
        int bytesWritten = resuming ? continueWriteToSocket(bufferOut) : writeToSocket(bufferOut);
        activeIOStats.onSocketWrite(this, bytesWritten, localWriteStartTime);
        boolean complete = !bufferOut.hasRemaining();
        if (writeWatermarkState.isEnabled()) {
            int consumed = bufferOut.position() - consumedBefore;
            // a builder's estimate is what was counted in, and it is not always what it built: settle the difference
            // on the last write, or the in-flight count drifts by that much per message
            writeWatermarkState.onDequeued(complete ? consumed + request.watermarkEnqueuedBytes - bufferOut.limit() : consumed);
        }
        if (complete) {
            safelyProcessCallback(request.messageSentCallback,
                    request.messageSendingContext,
                    bufferOut,
                    request.writeFuture,
                    request.localSendTimeInNanos);
        }
        return bytesWritten;
    }

    private void executeTask(Runnable task, BiConsumer<Runnable, Exception> taskCallback) {
        try {
            ioEventsListener.onTask(this, task, taskCallback);
        } catch (Exception ex) {
            taskCallback.accept(task, ex);
        }
    }

    private <C> void safelyProcessCallback(MessageSentCallback<C> messageSentCallback, C messageSendingContext,
                                           ByteBuffer bufferOut, CompletableFuture<C> writeFuture, long localSendTimeInNanos) {
        long methodCallStartTimeInNanos = activeIOStats.getTimeInNanos(IOStats.Operation.IO_EVENTS_LISTENER_ON_WRITE);
        try {
            if (messageSentCallback != null) {
                messageSentCallback.onMessageWriteCallback(bufferOut, null, messageSendingContext);
            } else if (writeFuture != null) {
                writeFuture.complete(messageSendingContext);
            } else {
                ioEventsListener.onWrite(this, bufferOut);
            }
        } catch (Exception processingException) {
            log.warn("Failed to process write callback on IO session {}", id, processingException);
        }
        activeIOStats.onIOEventsListenerOnWriteCallback(this, messageSendingContext, methodCallStartTimeInNanos);
        activeIOStats.onMessageSent(this, bufferOut, messageSendingContext, localSendTimeInNanos);
        releaseSendingContext(messageSendingContext);
    }

    private <C> void safelyProcessCallbackOnIOException(IOException ex, CompletableFuture<C> writeFuture, MessageSentCallback<C> messageSentCallback, C messageSendingContext, ByteBuffer bufferOut) {
        try {
            if (messageSentCallback != null) {
                messageSentCallback.onMessageWriteCallback(bufferOut, ex, messageSendingContext);
            } else if (writeFuture != null) {
                writeFuture.completeExceptionally(ex);
            } else {
                ioEventsListener.onWriteFailure(this, bufferOut);
            }
        } catch (Exception processingException) {
            log.warn("Failed to process callback when processing IOException", processingException);
        }
        releaseSendingContext(messageSendingContext);
    }

    private void releaseSendingContext(Object messageSendingContext) {
        if (messageSendingContext instanceof ReleasableMessageSendingContext) {
            try {
                ((ReleasableMessageSendingContext) messageSendingContext).release();
            } catch (Exception releaseException) {
                log.warn("Failed to release message sending context on IO session {}", id, releaseException);
            }
        }
    }

    protected int writeToSocket(ByteBuffer bufferOut) throws IOException {
        bufferOut.flip();
        return continueWriteToSocket(bufferOut);
    }

    protected int continueWriteToSocket(ByteBuffer bufferOut) throws IOException {
        int bytesWritten = 0;
        int lastWrite;
        do {
            lastWrite = socket.write(bufferOut);
            bytesWritten += lastWrite;
        } while (lastWrite > 0 && bufferOut.hasRemaining());
        return bytesWritten;
    }

    private boolean hasUnsentMessages() {
        // an empty ring is not the end of the writing: the last message can be half out, held as the pending write
        return ioThreadRequests.isNotEmpty() || pendingWrite.isNotFullyWritten();
    }

    void disconnect() {
        disconnect(true);
    }

    void disconnect(boolean releasePooledBuffers) {
        if (hasActiveSelectionKey()) {
            selectionKey.cancel();
        }
        if (socket.isOpen()) {
            try {
                socket.close();
            } catch (IOException e) {
                log.info("Failed to close socket on IOSession {}", id, e);
            }
        }
        if (pendingWrite.isNotFullyWritten()) {
            safelyProcessCallbackOnDisconnection(pendingWrite);
            pendingWrite.cleanRegularIOWriteOperation();
        }
        ioThreadRequests.forEach(request -> {
            if (request.isIOWriteOperation() || request.isIOWriteOperationWithByteBufferBuilder()) {
                safelyProcessCallbackOnDisconnection(request);
            } else {
                request.taskCallback.accept(request.task, new EOFException("Cancelled task " + request.task.getClass().getName() + " on IOSession " + id + " due to disconnection"));
            }
        });
        ioThreadRequests.clear();
        ioEventsListener.onDisconnected(this);
        activeIOStats.onSessionClosed(this);
        if (releasePooledBuffers) {
            // last, and not before the cancelled writes above: their callbacks are handed the very buffers this pool
            // owns, and on a direct pool that memory is freed here, not garbage collected
            releasePooledBuffers();
        }
    }

    private void safelyProcessCallbackOnDisconnection(IOThreadRequest threadRequest) {
        safelyProcessCallbackOnIOException(new EOFException("Cancelled socket write on IOSession " + id + " due to disconnection"),
                threadRequest.writeFuture, threadRequest.messageSentCallback, threadRequest.messageSendingContext, threadRequest.byteBuffer);
    }

    void releasePooledBuffers() {
        if (!sharedIOBufferPool) {
            ioBufferPool.stop();
        }
    }

    private interface IOThreadSender {

        boolean send(ByteBuffer message, IOWriter.MessageSentCallback messageSentCallback, Object messageSendingContext,
                     CompletableFuture writeFuture, boolean ioBufferPoolByteBuffer);
    }

    private interface WriteWatermarkStateTracker {

        void onEnqueued(int bytesCount);

        void onDequeued(int bytesCount);

        default boolean isEnabled() {
            return true;
        }
    }

    @Getter
    public static class CpuTimeStats {
        private long readCpuTimeInNanos;
        private long tasksCpuTimeInNanos;
        private long writeCpuTimeInNanos;

        void reset() {
            readCpuTimeInNanos = tasksCpuTimeInNanos = writeCpuTimeInNanos = 0;
        }

        void transfer(CpuTimeStats other) {
            other.readCpuTimeInNanos = readCpuTimeInNanos;
            other.tasksCpuTimeInNanos = tasksCpuTimeInNanos;
            other.writeCpuTimeInNanos = writeCpuTimeInNanos;
        }
    }

    @ToString(callSuper = true)
    private static class IOThreadRequest {
        private ByteBuffer byteBuffer;
        private ByteBufferBuilder byteBufferBuilder;
        private MessageSentCallback messageSentCallback;
        private Object messageSendingContext;
        private boolean ioBufferPoolByteBuffer;
        private long localSendTimeInNanos;
        private int watermarkEnqueuedBytes;
        private CompletableFuture writeFuture;
        private Runnable task;
        private BiConsumer<Runnable, Exception> taskCallback;

        private IOThreadRequest(int index) {
            // nothing to do
        }

        void translate(ByteBuffer byteBuffer, Boolean ioBufferPoolByteBuffer, long localSendTimeInNanos) {
            this.localSendTimeInNanos = localSendTimeInNanos;
            this.byteBuffer = byteBuffer;
            this.ioBufferPoolByteBuffer = ioBufferPoolByteBuffer;
        }

        void translate(ByteBuffer byteBuffer, CompletableFuture<?> writeFuture, Object messageSendingContext, Boolean ioBufferPoolByteBuffer, long localSendTimeInNanos) {
            this.localSendTimeInNanos = localSendTimeInNanos;
            this.byteBuffer = byteBuffer;
            this.ioBufferPoolByteBuffer = ioBufferPoolByteBuffer;
            this.messageSendingContext = messageSendingContext;
            this.writeFuture = writeFuture;
        }

        void translate(ByteBuffer byteBuffer, MessageSentCallback<?> messageSentCallback, Object messageSendingContext, Boolean ioBufferPoolByteBuffer, long localSendTimeInNanos) {
            this.localSendTimeInNanos = localSendTimeInNanos;
            this.byteBuffer = byteBuffer;
            this.ioBufferPoolByteBuffer = ioBufferPoolByteBuffer;
            this.messageSendingContext = messageSendingContext;
            this.messageSentCallback = messageSentCallback;
        }

        void translate(ByteBufferBuilder byteBufferBuilder, MessageSentCallback<?> messageSentCallback, Object messageSendingContext, long localSendTimeInNanos) {
            this.localSendTimeInNanos = localSendTimeInNanos;
            this.byteBufferBuilder = byteBufferBuilder;
            this.messageSendingContext = messageSendingContext;
            this.messageSentCallback = messageSentCallback;
        }

        void translate(Runnable task, BiConsumer<Runnable, Exception> taskCallback) {
            this.task = task;
            this.taskCallback = taskCallback;
        }

        boolean isNotFullyWritten() {
            return byteBuffer != null && byteBuffer.hasRemaining();
        }

        boolean isFullyWritten() {
            return byteBuffer == null || !byteBuffer.hasRemaining();
        }

        boolean isIOWriteOperation() {
            return this.byteBuffer != null;
        }

        boolean isIOWriteOperationWithByteBufferBuilder() {
            return this.byteBufferBuilder != null;
        }

        void cleanRegularIOWriteOperation() {
            this.byteBuffer = null;
            this.messageSentCallback = null;
            this.messageSendingContext = null;
            this.writeFuture = null;
        }

        void cleanByteBufferBuilderIOWriteOperation() {
            this.byteBufferBuilder = null;
            this.messageSentCallback = null;
            this.messageSendingContext = null;
        }

        void cleanIOThreadTask() {
            this.task = null;
            this.taskCallback = null;
        }

        void transferFromPoll(IOThreadRequest other) {
            if (other.byteBuffer != null) {
                this.byteBuffer = other.byteBuffer;
                this.ioBufferPoolByteBuffer = other.ioBufferPoolByteBuffer;
                this.localSendTimeInNanos = other.localSendTimeInNanos;
                this.watermarkEnqueuedBytes = other.watermarkEnqueuedBytes;
                this.messageSentCallback = other.messageSentCallback;
                this.messageSendingContext = other.messageSendingContext;
                this.writeFuture = other.writeFuture;
                other.cleanRegularIOWriteOperation();
            } else if (other.byteBufferBuilder != null) {
                this.byteBufferBuilder = other.byteBufferBuilder;
                this.localSendTimeInNanos = other.localSendTimeInNanos;
                this.messageSentCallback = other.messageSentCallback;
                this.messageSendingContext = other.messageSendingContext;
                other.cleanByteBufferBuilderIOWriteOperation();
            } else if (other.task != null) {
                this.task = other.task;
                this.taskCallback = other.taskCallback;
                other.cleanIOThreadTask();
            } else {
                throw new IllegalStateException("Should never have happened, we have a concurrency bug " + this);
            }
        }
    }

    private static class InactiveWriteWatermarkStateTracker implements WriteWatermarkStateTracker {

        static final WriteWatermarkStateTracker INSTANCE = new InactiveWriteWatermarkStateTracker();

        private InactiveWriteWatermarkStateTracker() {

        }

        @Override
        public void onEnqueued(int bytesCount) {
            // nothing to do
        }

        @Override
        public void onDequeued(int bytesCount) {
            // nothing to do
        }

        @Override
        public boolean isEnabled() {
            return false;
        }
    }

    private static class ActiveWriteWatermarkStateTracker implements WriteWatermarkStateTracker {
        private final int writeHighWatermark;
        private final int writeLowWatermark;
        private final AtomicInteger inFlightBytesToWrite;
        private final IOSession ioSession;
        private final IOEventsListener ioEventsListener;
        // the sender thread counts the enqueues and the IO worker the dequeues
        private final AtomicBoolean highWatermarkReached;

        public ActiveWriteWatermarkStateTracker(int writeHighWatermark, int writeLowWatermark, AtomicInteger inFlightBytesToWrite,
                                                IOSession ioSession, IOEventsListener ioEventsListener) {
            if (writeHighWatermark < writeLowWatermark) {
                throw new IllegalStateException("writeHighWatermark cannot be smaller than writeLowWatermark");
            }
            this.writeHighWatermark = writeHighWatermark;
            this.writeLowWatermark = writeLowWatermark;
            this.inFlightBytesToWrite = inFlightBytesToWrite;
            this.ioSession = ioSession;
            this.ioEventsListener = ioEventsListener;
            this.highWatermarkReached = new AtomicBoolean();
        }

        @Override
        public void onEnqueued(int bytesCount) {
            // what the add returned, never a second read: the other thread moves the same counter, and re-reading it
            // reports a total that has already left the edge being announced
            int inFlightBytes = inFlightBytesToWrite.addAndGet(bytesCount);
            if (inFlightBytes >= writeHighWatermark && highWatermarkReached.compareAndSet(false, true)) {
                ioEventsListener.onWatermarkEvent(ioSession, true, inFlightBytes);
            }
        }

        @Override
        public void onDequeued(int bytesCount) {
            int inFlightBytes = inFlightBytesToWrite.addAndGet(-bytesCount);
            if (inFlightBytes <= writeLowWatermark && highWatermarkReached.compareAndSet(true, false)) {
                ioEventsListener.onWatermarkEvent(ioSession, false, inFlightBytes);
            }
        }
    }

    private class InactiveIOWriter implements IOWriter {

        @Override
        public ByteBuffer borrow(int capacity) {
            // we still allow to allocate bytes but this time not from the pool
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void unborrow(ByteBuffer borrowed) {
            // nothing to do
        }

        @Override
        public ByteBuffer allocate(int capacity) {
            // we still allow to allocate bytes but this time not from the pool
            return ByteBuffer.allocate(capacity);
        }

        @Override
        public void send(byte[] message) {
            ioEventsListener.onWriteFailure(IOSessionImpl.this, ByteBuffer.wrap(message).flip());
        }

        @Override
        public void send(ByteBuffer message, boolean ioBufferPoolByteBuffer) {
            ioEventsListener.onWriteFailure(IOSessionImpl.this, message.flip());
        }

        @Override
        public <C> CompletableFuture<C> send(ByteBuffer message, C messageSendingContext, boolean ioBufferPoolByteBuffer) {
            return CompletableFuture.failedFuture(illegalStateCall());
        }

        @Override
        public <C> void send(ByteBuffer message, C messageSendingContext, MessageSentCallback<C> messageSentCallback, boolean ioBufferPoolByteBuffer) {
            messageSentCallback.onMessageWriteCallback(message.flip(), illegalStateCall(), messageSendingContext);
        }

        @Override
        public <C> void send(ByteBufferBuilder byteBufferBuilder, C messageSendingContext, MessageSentCallback<C> messageSentCallback) {
            ByteBuffer message = null;
            try {
                message = byteBufferBuilder.build();
                messageSentCallback.onMessageWriteCallback(message.flip(), illegalStateCall(), messageSendingContext);
            } catch (IOException ex) {
                messageSentCallback.onMessageWriteCallback(null, ex, messageSendingContext);
            } finally {
                if (message != null && byteBufferBuilder.isPooledByteBuffer()) {
                    ioBufferPool.returnByteBuffer(message);
                }
            }
        }

        private IllegalStateException illegalStateCall() {
            return new IllegalStateException("IO session is inactive");
        }
    }

    private class ActiveIOWriter implements IOWriter {

        private final RingBuffer.EventTranslatorFourArg<IOThreadRequest, ByteBuffer, MessageSentCallback<?>, Object, Boolean> translateSendWithCallback = this::translateSendWithCallback;
        private final RingBuffer.EventTranslatorFourArg<IOThreadRequest, ByteBuffer, CompletableFuture<?>, Object, Boolean> translateSendWithFuture = this::translateSendWithFuture;
        private final RingBuffer.EventTranslatorTwoArg<IOThreadRequest, ByteBuffer, Boolean> translateSend = this::translateSend;
        private final RingBuffer.EventTranslatorThreeArg<IOThreadRequest, ByteBufferBuilder, MessageSentCallback<?>, Object> translateSendWithByteBufferBuilder = this::translateSendByteBufferBuilder;
        private final IOSession ioSession = IOSessionImpl.this;
        private final IOThreadSender ioThreadSender = orderedWrites ? this::declineSendOnIOThread : this::sendOnIOThread;

        @Override
        public ByteBuffer borrow(int capacity) {
            return ioBufferPool.borrowByteBuffer(capacity);
        }

        @Override
        public void unborrow(ByteBuffer borrowed) {
            ioBufferPool.returnByteBuffer(borrowed);
        }

        @Override
        public ByteBuffer allocate(int capacity) {
            return ioBufferPool.newByteBuffer(capacity);
        }

        private void translateSend(IOThreadRequest wr, ByteBuffer byteBuffer, Boolean ioBufferPoolByteBuffer) {
            // io stats time not 100% accurate, but cannot give it in method call as translator would create a Long object
            wr.translate(byteBuffer, ioBufferPoolByteBuffer, activeIOStats.getTimeInNanos(IOStats.Operation.IO_MESSAGE_WRITE));
        }

        private void translateSendByteBufferBuilder(IOThreadRequest wr, ByteBufferBuilder byteBufferBuilder, MessageSentCallback<?> messageSentCallback,
                                                    Object messageSendingContext) {
            wr.translate(byteBufferBuilder, messageSentCallback, messageSendingContext, activeIOStats.getTimeInNanos(IOStats.Operation.IO_MESSAGE_WRITE));
        }

        private void translateSendWithFuture(IOThreadRequest wr, ByteBuffer byteBuffer, CompletableFuture<?> writeFuture,
                                             Object messageSendingContext, Boolean ioBufferPoolByteBuffer) {
            wr.translate(byteBuffer, writeFuture, messageSendingContext, ioBufferPoolByteBuffer,
                    activeIOStats.getTimeInNanos(IOStats.Operation.IO_MESSAGE_WRITE));
        }

        private void translateSendWithCallback(IOThreadRequest wr, ByteBuffer byteBuffer, MessageSentCallback<?> messageSentCallback,
                                               Object messageSendingContext, Boolean ioBufferPoolByteBuffer) {
            wr.translate(byteBuffer, messageSentCallback, messageSendingContext, ioBufferPoolByteBuffer,
                    activeIOStats.getTimeInNanos(IOStats.Operation.IO_MESSAGE_WRITE));
        }

        @Override
        public <C> CompletableFuture<C> send(ByteBuffer message, C messageSendingContext, boolean ioBufferPoolByteBuffer) {
            if (Thread.currentThread() == ioWorkerThread) {
                CompletableFuture<C> ioThreadFuture = new CompletableFuture<>();
                if (pendingWrite.isFullyWritten()
                        && ioThreadSender.send(message, null, messageSendingContext, ioThreadFuture, ioBufferPoolByteBuffer)) {
                    return ioThreadFuture;
                }
                int enqueuedBytes = onWriteQueueing(message);
                if (offerOrDrainAndRetry(translateSendWithFuture, message, ioThreadFuture, messageSendingContext, ioBufferPoolByteBuffer)) {
                    registerWriteOperationIfNeeded();
                    return ioThreadFuture;
                }
                onWriteQueueingFailed(enqueuedBytes);
                return CompletableFuture.failedFuture(writeRingFull());
            }
            CompletableFuture<C> future = new CompletableFuture<>();
            onWriteQueueing(message);
            ioThreadRequests.offerBlocking(translateSendWithFuture, message, future, messageSendingContext, ioBufferPoolByteBuffer, ioThreadSocketWriteRequestsIdleStrategy);
            registerWriteOperationIfNeeded();
            return future;
        }

        @Override
        public <C> void send(ByteBuffer message, C messageSendingContext, MessageSentCallback<C> messageSentCallback, boolean ioBufferPoolByteBuffer) {
            if (Thread.currentThread() == ioWorkerThread) {
                if (sendInIOThread(message, messageSendingContext, messageSentCallback, ioBufferPoolByteBuffer)) {
                    return;
                }
                int enqueuedBytes = onWriteQueueing(message);
                if (offerOrDrainAndRetry(translateSendWithCallback, message, messageSentCallback, messageSendingContext, ioBufferPoolByteBuffer)) {
                    registerWriteOperationIfNeeded();
                    return;
                }
                onWriteQueueingFailed(enqueuedBytes);
                safelyProcessCallbackOnIOException(writeRingFull(), null, messageSentCallback, messageSendingContext, message);
                return;
            }
            onWriteQueueing(message);
            ioThreadRequests.offerBlocking(translateSendWithCallback, message, messageSentCallback, messageSendingContext, ioBufferPoolByteBuffer, ioThreadSocketWriteRequestsIdleStrategy);
            registerWriteOperationIfNeeded();
        }

        @Override
        public void send(byte[] message) {
            send(ioBufferPool.borrowByteBuffer(message.length).put(message), true);
        }

        @Override
        public void send(ByteBuffer message, boolean ioBufferPoolByteBuffer) {
            if (Thread.currentThread() == ioWorkerThread) {
                if (sendInIOThread(message, null, null, ioBufferPoolByteBuffer)) {
                    return;
                }
                int enqueuedBytes = onWriteQueueing(message);
                if (offerOrDrainAndRetry(translateSend, message, ioBufferPoolByteBuffer)) {
                    registerWriteOperationIfNeeded();
                    return;
                }
                onWriteQueueingFailed(enqueuedBytes);
                ioEventsListener.onWriteFailure(ioSession, message.flip());
                return;
            }
            onWriteQueueing(message);
            ioThreadRequests.offerBlocking(translateSend, message, ioBufferPoolByteBuffer, ioThreadSocketWriteRequestsIdleStrategy);
            registerWriteOperationIfNeeded();
        }

        @Override
        public <C> void send(ByteBufferBuilder byteBufferBuilder, C messageSendingContext, MessageSentCallback<C> messageSentCallback) {
            if (Thread.currentThread() == ioWorkerThread) {
                // !paused as well: this branch builds the message before offering it, and a declined send would have
                // nowhere to put one the builder can no longer be asked for a second time
                if (!orderedWrites && !paused && pendingWrite.isFullyWritten()) {
                    try {
                        ioThreadSender.send(byteBufferBuilder.build(), messageSentCallback, messageSendingContext, null, byteBufferBuilder.isPooledByteBuffer());
                    } catch (IOException ex) {
                        safelyProcessCallbackOnIOException(ex, null, messageSentCallback, messageSendingContext, null);
                    }
                    return;
                }
                if (writeWatermarkState.isEnabled()) {
                    writeWatermarkState.onEnqueued(byteBufferBuilder.getEstimatedByteBufferSize());
                }
                if (offerOrDrainAndRetry(translateSendWithByteBufferBuilder, byteBufferBuilder, messageSentCallback, messageSendingContext)) {
                    registerWriteOperationIfNeeded();
                    return;
                }
                safelyProcessCallbackOnIOException(writeRingFull(), null, messageSentCallback, messageSendingContext, null);
                return;
            }
            if (writeWatermarkState.isEnabled()) {
                // important enqueue first as the ioThreadRequests could be faster processing the byteBufferBuilder and call release() before calling getEstimatedByteBufferSize()
                writeWatermarkState.onEnqueued(byteBufferBuilder.getEstimatedByteBufferSize());
            }
            ioThreadRequests.offerBlocking(translateSendWithByteBufferBuilder, byteBufferBuilder, messageSentCallback, messageSendingContext, ioThreadSocketWriteRequestsIdleStrategy);
            registerWriteOperationIfNeeded();
        }

        private void registerWriteOperationIfNeeded() {
            if (paused) {
                return;
            }
            try {
                selectionKey.interestOpsOr(SelectionKey.OP_WRITE);
                wakeupSelectorIfNeeded.run();
            } catch (CancelledKeyException ex) {
                // Either session is being migrated (selectionKey briefly cancelled between
                // detach and re-attach) or session is fully stopped. Retry while alive — the
                // volatile selectionKey will eventually point to the new key after attach.
                if (!started.get()) {
                    return;
                }
                Thread.yield();
                registerWriteOperationIfNeeded();
            }
        }

        private <C> boolean sendInIOThread(ByteBuffer message, C messageSendingContext, MessageSentCallback<C> messageSentCallback, boolean ioBufferPoolByteBuffer) {
            return pendingWrite.isFullyWritten() && ioThreadSender.send(message, messageSentCallback, messageSendingContext, null, ioBufferPoolByteBuffer);
        }

        private <A, B> boolean offerOrDrainAndRetry(RingBuffer.EventTranslatorTwoArg<IOThreadRequest, A, B> translator,
                                                    A first, B second) {
            return ioThreadRequests.offer(translator, first, second)
                    || (drainWriteQueue() && ioThreadRequests.offer(translator, first, second));
        }

        private <A, B, C> boolean offerOrDrainAndRetry(RingBuffer.EventTranslatorThreeArg<IOThreadRequest, A, B, C> translator,
                                                       A first, B second, C third) {
            return ioThreadRequests.offer(translator, first, second, third)
                    || (drainWriteQueue() && ioThreadRequests.offer(translator, first, second, third));
        }

        private <A, B, C, D> boolean offerOrDrainAndRetry(RingBuffer.EventTranslatorFourArg<IOThreadRequest, A, B, C, D> translator,
                                                          A first, B second, C third, D fourth) {
            return ioThreadRequests.offer(translator, first, second, third, fourth)
                    || (drainWriteQueue() && ioThreadRequests.offer(translator, first, second, third, fourth));
        }

        private boolean drainWriteQueue() {
            if (insideWriteCycle || paused) {
                return false;
            }
            try {
                onOperationWrite();
                return true;
            } catch (IOException ex) {
                // the session is finished, but this is not where that is handled: let the send fail like any other and
                // leave the disconnection to the IO worker's own error path
                log.debug("Failed to drain the write queue of IO session {}: {}", id, ex.getMessage());
                return false;
            }
        }

        private int onWriteQueueing(ByteBuffer message) {
            if (!writeWatermarkState.isEnabled()) {
                return 0;
            }
            int enqueuedBytes = message.position();
            writeWatermarkState.onEnqueued(enqueuedBytes);
            return enqueuedBytes;
        }

        private void onWriteQueueingFailed(int enqueuedBytes) {
            if (writeWatermarkState.isEnabled()) {
                writeWatermarkState.onDequeued(enqueuedBytes);
            }
        }

        private IOException writeRingFull() {
            return new IOException("Cannot queue a write on IOSession " + id + ": the write ring is full and writing "
                    + "what it holds did not free it, the peer is not reading");
        }

        private boolean declineSendOnIOThread(ByteBuffer message, MessageSentCallback messageSentCallback, Object messageSendingContext,
                                              CompletableFuture writeFuture, boolean ioBufferPoolByteBuffer) {
            return false;
        }

        private boolean sendOnIOThread(ByteBuffer message, MessageSentCallback messageSentCallback, Object messageSendingContext,
                                       CompletableFuture writeFuture, boolean ioBufferPoolByteBuffer) {
            if (paused) {
                // a paused session puts nothing on the socket, and the selector is not what stops this write: queueing
                // it is, and it leaves on resume
                return false;
            }
            pendingWrite.byteBuffer = message;
            pendingWrite.ioBufferPoolByteBuffer = ioBufferPoolByteBuffer;
            pendingWrite.messageSentCallback = messageSentCallback;
            pendingWrite.messageSendingContext = messageSendingContext;
            pendingWrite.writeFuture = writeFuture;
            pendingWrite.localSendTimeInNanos = activeIOStats.getTimeInNanos(IOStats.Operation.IO_MESSAGE_WRITE);
            pendingWrite.watermarkEnqueuedBytes = message.position();
            if (writeWatermarkState.isEnabled()) {
                writeWatermarkState.onEnqueued(pendingWrite.watermarkEnqueuedBytes);
            }
            try {
                writeBufferOut(pendingWrite, false, activeIOStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_WRITE));
            } catch (IOException ex) {
                safelyProcessCallbackOnIOException(ex, writeFuture, messageSentCallback, messageSendingContext, message);
                discardRemaining(message);
            } finally {
                if (message.hasRemaining()) {
                    registerWriteOperationIfNeeded();
                } else {
                    if (ioBufferPoolByteBuffer) {
                        ioBufferPool.returnByteBuffer(message);
                    }
                    // only if a send from the callback has not already made the slot its own
                    if (pendingWrite.byteBuffer == message) {
                        pendingWrite.cleanRegularIOWriteOperation();
                    }
                }
            }
            return true;
        }
    }
}