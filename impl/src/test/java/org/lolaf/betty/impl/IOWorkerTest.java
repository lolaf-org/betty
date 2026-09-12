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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.ss.IdleStrategySelectStrategy;
import org.lolaf.ringos.Deadline;
import org.lolaf.ringos.idling.BusySpinIdleStrategy;
import org.lolaf.ringos.idling.IdleStrategy;
import org.lolaf.ringos.idling.TimedWaitNotifyIdleStrategy;
import org.lolaf.ringos.idling.WaitNotifyIdleStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * {@link IOWorkerImpl} on its own: one worker, one socket pair, and the test thread acting as the peer. Nothing here
 * builds a {@link org.lolaf.betty.api.Client} or a {@link org.lolaf.betty.api.Server} - a session is registered
 * straight onto the worker, so what a test asserts is the worker's own behaviour rather than a connector's.
 *
 * <p>The contract most of these are about is what {@code stop()} promises: <b>when it returns, the IO thread is
 * dead</b>. Everything a caller does next - releasing buffers, freeing off-heap memory, unmapping a memory mapped
 * file - is unsafe while a thread can still deliver a read, and a caller handed back control has no way left to
 * defend itself. The stop deadline therefore governs the graceful part of the shutdown, not that final wait.
 */
class IOWorkerTest {

    /**
     * Deliberately generous, so that a stop waiting it out is unmistakable rather than a slow test.
     */
    private static final Duration GENEROUS_DEADLINE = Duration.ofSeconds(4);

    private ServerSocketChannel peerListener;
    private SocketChannel peer;
    private SocketChannel registeredSocket;
    private IOWorkerImpl worker;

    private static void close(java.nio.channels.Channel channel) throws IOException {
        if (channel != null && channel.isOpen()) {
            channel.close();
        }
    }

    @BeforeEach
    void before() throws IOException {
        peerListener = ServerSocketChannel.open();
        peerListener.bind(new InetSocketAddress("localhost", 0));
    }

    @AfterEach
    void after() throws IOException {
        if (worker != null && worker.isStarted()) {
            worker.stop(Deadline.immediate());
        }
        close(peer);
        close(registeredSocket);
        close(peerListener);
    }

    private IOWorkerImpl startWorker(String name) {
        worker = new IOWorkerImpl(name, IOWorkersGroupSettings.builder().id(name).build(),
                IOWorkersGroupSettings.IOThreadGroup.builder().build());
        return worker.start();
    }

    /**
     * Opens a socket pair on the loopback and hands one end to the worker, keeping the other as {@link #peer} for the
     * test to write into. Writing there is what makes the worker's {@code onRead} fire.
     */
    private IOSession registerSession(IOEventsListener listener) throws IOException {
        registeredSocket = SocketChannel.open(peerListener.getLocalAddress());
        peer = peerListener.accept();
        worker.register(true, registeredSocket, ClientBuilder.builder().id("io-worker-test").ioEventsListener(listener).build());
        await().atMost(Duration.ofSeconds(10)).until(() -> worker.getRegisteredSessionsCount() == 1);
        return worker.getRegisteredSessions().get(0);
    }

    private void writeToPeer(String message) throws IOException {
        peer.write(ByteBuffer.wrap(message.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    }

    @Test
    void startRunsTheIOThreadAndStopEndsIt() {
        startWorker("lifecycle-worker");

        assertThat(worker.isStarted()).isTrue();
        assertThat(worker.isThreadAlive()).isTrue();

        worker.stop(Deadline.immediate());

        assertThat(worker.isStarted()).isFalse();
        assertThat(worker.isThreadAlive()).as("the IO thread is dead by the time stop returns").isFalse();
    }

    @Test
    void stoppingTwiceIsANoOpTheSecondTime() {
        startWorker("double-stop-worker");

        worker.stop(Deadline.immediate());
        worker.stop(Deadline.immediate());

        assertThat(worker.isStarted()).isFalse();
        assertThat(worker.isThreadAlive()).isFalse();
    }

    @Test
    void aRegisteredSessionIsCountedUntilItStops() throws IOException {
        startWorker("registration-worker");

        IOSession session = registerSession(new CountingListener());
        assertThat(worker.getRegisteredSessions()).containsExactly(session);

        session.stop(Deadline.immediate());

        await().atMost(Duration.ofSeconds(10)).until(() -> worker.getRegisteredSessionsCount() == 0);
    }

    /**
     * The defect this class was written for: an IO thread held inside {@code onRead} past the stop deadline used to be
     * left running - {@code stop} logged a warning, closed the selector under it and returned as though it had
     * stopped, which is how a memory mapped store came to be unmapped while a read was still being delivered into it.
     *
     * <p>The deadline given here has to be one that has <em>not</em> run out by the time the join is reached, which is
     * what makes the defect visible: with an exhausted deadline the old code called {@code Thread.join(0)} and waited
     * for ever, accidentally doing the right thing. Both are wrong, and the second is the hang seen in the field.
     */
    @Test
    void aWorkerStillInsideACallbackIsDeadBeforeStopReturns() throws Exception {
        CountDownLatch enteredCallback = new CountDownLatch(1);
        CountDownLatch releaseCallback = new CountDownLatch(1);
        BlockingListener listener = new BlockingListener(enteredCallback, releaseCallback);

        startWorker("blocked-callback-worker");
        registerSession(listener);
        writeToPeer("wake the worker up");

        assertThat(enteredCallback.await(10, TimeUnit.SECONDS)).as("the IO thread is inside onRead").isTrue();
        // it stays there well past the deadline the stop below is given
        Thread releaser = new Thread(() -> {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            releaseCallback.countDown();
        }, "callback-releaser");
        releaser.setDaemon(true);
        releaser.start();

        worker.stop(Deadline.of(Duration.ofMillis(200)));

        assertThat(listener.callbackReturned).as("the callback the IO thread was running has finished").isTrue();
        assertThat(worker.isThreadAlive()).as("the IO thread outlived the stop that returned").isFalse();
    }

    /**
     * A worker stopped from one of its own callbacks cannot wait for itself: the thread only leaves the run loop once
     * that callback unwinds, so a join would never return. Nothing is released under a live thread either - the caller
     * <em>is</em> that thread.
     */
    @Test
    void stopCalledFromItsOwnIOThreadDoesNotWaitForItself() throws Exception {
        AtomicLong stopDurationInMillis = new AtomicLong(-1);
        CountDownLatch stopped = new CountDownLatch(1);

        startWorker("self-stopping-worker");
        IOWorkerImpl stoppingWorker = worker;
        registerSession(new IOEventsListener() {
            @Override
            public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
                message.position(message.limit());
                long startInNanos = System.nanoTime();
                stoppingWorker.stop(Deadline.of(GENEROUS_DEADLINE));
                stopDurationInMillis.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startInNanos));
                stopped.countDown();
            }
        });
        writeToPeer("stop yourself");

        assertThat(stopped.await(30, TimeUnit.SECONDS)).as("the worker stopped itself").isTrue();
        assertThat(stopDurationInMillis.get())
                .as("stopping from the IO thread must not wait for the thread the caller is on")
                .isLessThan(GENEROUS_DEADLINE.dividedBy(2).toMillis());
    }

    /**
     * A session stopped from inside its own read callback, which is how a protocol usually ends one - a Logout arrives
     * and the handler takes the connection down.
     *
     * <p>Neither wait {@link IOSessionImpl#stop} makes can be satisfied by another thread there: the read flag it
     * watches is the read on the stack, and the write queue it watches is drained by this very thread. Waiting them
     * out parks the IO worker for the whole deadline - and with it every other session on that thread - to flush
     * nothing. So the stop has to come back promptly however generous the deadline is.
     */
    @Test
    void stoppingASessionFromItsOwnReadCallbackDoesNotWaitOutTheDeadline() throws Exception {
        AtomicLong stopDurationInMillis = new AtomicLong(-1);
        CountDownLatch stopped = new CountDownLatch(1);

        startWorker("self-stopping-session-worker");
        registerSession(new IOEventsListener() {
            @Override
            public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
                message.position(message.limit());
                long startInNanos = System.nanoTime();
                session.stop(Deadline.of(GENEROUS_DEADLINE));
                stopDurationInMillis.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startInNanos));
                stopped.countDown();
            }
        });
        writeToPeer("close yourself");

        assertThat(stopped.await(30, TimeUnit.SECONDS)).as("the session stopped itself").isTrue();
        assertThat(stopDurationInMillis.get())
                .as("stopping from the IO thread must not wait out the %s deadline", GENEROUS_DEADLINE)
                .isLessThan(GENEROUS_DEADLINE.dividedBy(2).toMillis());
    }

    /**
     * The same contract with nothing exotic holding the thread: a session being read continuously while stop is
     * called, which is the shape of a connector shut down under load.
     */
    @Test
    void noIOThreadOutlivesAStopUnderLoad() throws Exception {
        CountingListener listener = new CountingListener();

        startWorker("loaded-worker");
        registerSession(listener);

        AtomicBoolean sending = new AtomicBoolean(true);
        AtomicReference<Exception> producerFailure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            ByteBuffer payload = ByteBuffer.allocate(512);
            while (sending.get()) {
                try {
                    payload.clear();
                    peer.write(payload);
                } catch (IOException e) {
                    return; // the socket went away under us, which is the point of the test
                } catch (RuntimeException e) {
                    producerFailure.set(e);
                    return;
                }
            }
        }, "io-worker-test-producer");
        producer.setDaemon(true);
        producer.start();

        await().atMost(Duration.ofSeconds(10)).until(() -> listener.bytesRead.get() > 0);

        worker.stop(Deadline.of(Duration.ofSeconds(2)));

        assertThat(worker.isThreadAlive()).as("the IO thread outlived a stop under load").isFalse();
        sending.set(false);
        producer.join(TimeUnit.SECONDS.toMillis(5));
        assertThat(producerFailure).hasValue(null);
    }

    /**
     * An {@code onRead} that holds the IO thread for as long as the test tells it to, and does not let an interrupt
     * cut it short - a callback doing real work, blocked on a lock or a disk write, behaves this way.
     */
    private static class BlockingListener implements IOEventsListener {
        private final CountDownLatch entered;
        private final CountDownLatch release;
        private final AtomicBoolean callbackReturned = new AtomicBoolean();

        BlockingListener(CountDownLatch entered, CountDownLatch release) {
            this.entered = entered;
            this.release = release;
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            message.position(message.limit());
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    release.await();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            callbackReturned.set(true);
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * {@code TimerSlackAwareBackoffIdleStrategy} narrows the OS timer slack of whichever thread calls {@code prctl},
     * so its {@code assignToThread} is only worth anything when the IO thread itself makes the call, before it starts
     * parking. Nothing else can do it on that thread's behalf - which is why this asserts who called, not just that
     * someone did.
     */
    @Test
    void theSelectStrategyIsAssignedToTheIOThreadBeforeItSelects() {
        AtomicReference<Thread> assignedThread = new AtomicReference<>();
        AtomicReference<Thread> callingThread = new AtomicReference<>();
        AtomicBoolean assignedBeforeFirstIdle = new AtomicBoolean();
        AtomicInteger idleCount = new AtomicInteger();

        IdleStrategy recordingIdleStrategy = new IdleStrategy() {
            @Override
            public void idle(int workCount) {
                idleCount.incrementAndGet();
                Thread.onSpinWait();
            }

            @Override
            public void idle() {
                idleCount.incrementAndGet();
                Thread.onSpinWait();
            }

            @Override
            public void reset() {
                // nothing to do
            }

            @Override
            public void assignToThread(Thread thread) {
                assignedBeforeFirstIdle.set(idleCount.get() == 0);
                assignedThread.set(thread);
                callingThread.set(Thread.currentThread());
            }
        };

        String name = "timer-slack-worker";
        worker = new IOWorkerImpl(name, IOWorkersGroupSettings.builder().id(name).build(),
                IOWorkersGroupSettings.IOThreadGroup.builder()
                        .selectStrategy(new IdleStrategySelectStrategy(recordingIdleStrategy))
                        .build());
        worker.start();

        await().atMost(Duration.ofSeconds(10)).until(() -> assignedThread.get() != null);

        assertThat(callingThread.get())
                .as("the IO thread has to make the call itself - prctl sets the slack of the calling thread")
                .isSameAs(assignedThread.get());
        assertThat(assignedThread.get().getName()).contains(name);
        assertThat(assignedBeforeFirstIdle).as("assigned before the loop ever idles").isTrue();
    }

    /**
     * A select loop signals nothing - it polls and idles on what the poll found - so a strategy that waits to be told
     * waits for good, and the worker silently stops selecting. Refused where it is configured, which is the only
     * point at which it still looks like a mistake rather than a hang.
     */
    @Test
    void aWaitNotifyIdleStrategyIsRefusedRatherThanLeftToStallTheWorker() {
        assertThatThrownBy(() -> new IdleStrategySelectStrategy(new WaitNotifyIdleStrategy()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("WaitNotifyIdleStrategy")
                .hasMessageContaining("stop");

        assertThatThrownBy(() -> new IdleStrategySelectStrategy(new TimedWaitNotifyIdleStrategy(Duration.ofMillis(1))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("TimedWaitNotifyIdleStrategy");

        assertThatThrownBy(() -> new IdleStrategySelectStrategy(null))
                .isInstanceOf(NullPointerException.class);

        assertThatCode(() -> new IdleStrategySelectStrategy(BusySpinIdleStrategy.getInstance()))
                .doesNotThrowAnyException();
    }

    private static class CountingListener implements IOEventsListener {
        private final AtomicLong bytesRead = new AtomicLong();

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            bytesRead.addAndGet(message.remaining());
            message.position(message.limit());
        }
    }
}
