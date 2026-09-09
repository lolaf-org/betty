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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOSessionTag;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.ringos.Deadline;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@Slf4j
class ClientImplTest extends AbstractTest {

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testEmbeddedSchedulerService(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnv(clientBuilder.toBuilder()
                .scheduledExecutorService(null)
                .build(), serverBuilder);

        startAndWaitForConnections();
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testConnectionFailoverWorks(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        InetSocketAddress secondServerAddress = new InetSocketAddress("localhost", TestPorts.findFree());
        clientBuilder = clientBuilder.toBuilder()
                .connectAddress(secondServerAddress)
                .connectAddress(serverBuilder.getBindAddress())
                .build();
        setupTestEnv(clientBuilder, serverBuilder);


        assertThat(client.isConnected()).isFalse();
        client.start();

        assertThat(client.isConnected()).isFalse();

        server.start();

        await().until(client::isConnected);

        await().untilAsserted(() -> assertThat(server.getIOSessions()).isNotEmpty());

        server.stop(Deadline.immediate());

        await().until(() -> !client.isConnected());

        server = serverBuilder.toBuilder()
                .bindAddress(secondServerAddress)
                .build().newInstance().start();

        await().until(client::isConnected);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testLargeMessageIsAppended(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {

        StringBuilder receivedFullyMessage = new StringBuilder();
        AtomicInteger receivedMessagesCount = new AtomicInteger();

        clientBuilder = clientBuilder.toBuilder()
                .ioEventsListener((session, message, localReceiveTimeInNanos) -> {
                    receivedMessagesCount.getAndIncrement();
                    byte[] content = new byte[message.remaining()];
                    message.get(content);
                    receivedFullyMessage.append(new String(content));
                }).build();
        setupTestEnv(clientBuilder, serverBuilder);

        client.start();
        server.start();

        String firstBigString = "a".repeat(32 * 1024);
        String secondBigString = "b".repeat(32 * 1024);
        String totalPayload = firstBigString + secondBigString;

        serverClientIOsession = waitForConnection(serverIoEventsListener);
        serverClientIOsession.send(firstBigString.getBytes());
        serverClientIOsession.send(secondBigString.getBytes());
        await().untilAsserted(() -> assertThat(receivedFullyMessage).hasToString(totalPayload));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testMessagesNotFullyDecodedAreProvidedAgainOnNextRead(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {

        AtomicReference<String> receivedFullyMessage = new AtomicReference<>();
        AtomicInteger receivedMessagesCount = new AtomicInteger();

        clientBuilder = clientBuilder.toBuilder()
                .ioEventsListener((session, message, localReceiveTimeInNanos) -> {
                    receivedMessagesCount.getAndIncrement();
                    byte[] content = new byte[message.remaining()];
                    message.get(content);
                    receivedFullyMessage.set(new String(content));
                    if (!new String(content).equals("fully decoded message")) {
                        // say we did not read anything, buffer.compact will be called after this methods call
                        message.position(0);
                    }
                }).build();
        setupTestEnv(clientBuilder, serverBuilder);

        client.start();
        server.start();

        serverClientIOsession = waitForConnection(serverIoEventsListener);
        serverClientIOsession.send("fully ".getBytes());
        await().untilAtomic(receivedMessagesCount, Matchers.equalTo(1));
        serverClientIOsession.send("decoded ".getBytes());
        await().untilAtomic(receivedMessagesCount, Matchers.equalTo(2));
        serverClientIOsession.send("message".getBytes());
        await().untilAtomic(receivedMessagesCount, Matchers.equalTo(3));
        await().untilAtomic(receivedFullyMessage, Matchers.equalTo("fully decoded message"));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testReadErrorAreTrapped(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        AtomicReference<Exception> trappedReadError = new AtomicReference<>();
        AtomicReference<Exception> trappedWriteError = new AtomicReference<>();
        setupTestEnv(clientBuilder.toBuilder()
                .ioEventsListener(new TestIOEventsListener() {
                    @Override
                    public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
                        throw new IllegalStateException("Test error");
                    }

                    @Override
                    public void onError(IOSession session, Exception error) {
                        trappedReadError.set(error);
                    }
                })
                .build(), serverBuilder.toBuilder()
                .ioEventsListener(new TestIOEventsListener() {

                    @Override
                    public void onWrite(IOSession session, ByteBuffer message) {
                        throw new IllegalStateException("Test write error");
                    }

                    @Override
                    public void onError(IOSession session, Exception error) {
                        trappedWriteError.set(error);
                    }
                })
                .build());

        startAndWaitForConnections();

        serverClientIOsession.send("test".getBytes());

        await().untilAtomic(trappedReadError, Matchers.any(Exception.class));
        await().untilAtomic(trappedWriteError, Matchers.any(Exception.class));
    }

    @Test
    void testWatermarkEvents() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder()
                .ioSettings(IOSettings.builder()
                        .writeHighWatermark(128)
                        .writeLowWatermark(16)
                        .build())
                .build(), getTestServerBuilder());

        clientSendMessagesAndWait("test", 1024);

        verify(clientIoEventsListener, atLeast(1)).onWatermarkEvent(eq(clientIOsession), eq(true), anyLong());
        verify(clientIoEventsListener, atLeast(1)).onWatermarkEvent(eq(clientIOsession), eq(false), anyLong());

        clearInvocations(clientIoEventsListener);

        clientIOsession.send(new byte[129]);

        await().untilAsserted(() -> verify(clientIoEventsListener, atLeast(1)).onWatermarkEvent(clientIOsession, true, 129L));
        await().untilAsserted(() -> verify(clientIoEventsListener, atLeast(1)).onWatermarkEvent(clientIOsession, false, 0L));
    }

    private AtomicInteger clientSendMessages(String message, int count) {
        AtomicInteger receivedBytes = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        for (int i = 0; i < count; i++) {
            clientIOsession.send(message.getBytes());
        }
        return receivedBytes;
    }

    private void clientSendMessagesAndWait(String message, int count) {
        AtomicInteger receivedBytes = clientSendMessages(message, count);
        int expectedReceivedBytesCount = count * message.length();
        await().untilAtomic(receivedBytes, Matchers.equalTo(expectedReceivedBytesCount));
    }


    @ParameterizedTest
    @MethodSource("getTestParams")
    void testClientStoppingTriggersServerIOListenerEvents(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnv(clientBuilder, serverBuilder);
        startAndWaitForConnections();

        clientIOsession.stop(Deadline.immediate());

        assertThat(client.isConnected()).isFalse();
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onShutdown(serverClientIOsession));

        verify(serverIoEventsListener, never()).onError(any(), any());
        verify(clientIoEventsListener, never()).onError(any(), any());
    }

    @Test
    void testWithMaxBytesCountPerWriteCPUCycle() {
        setupTestEnv(getTestClientBuilder().toBuilder()
                .ioSettings(IOSettings.builder().maxBytesCountPerWriteCycle(32).build())
                .build(), getTestServerBuilder());

        startAndWaitForConnections();

        AtomicInteger receivedBytes = clientSendMessages("test", 1024);
        await().untilAtomic(receivedBytes, Matchers.equalTo(1024 * "test".length()));
    }

    @Test
    void testClientStoppingCorrectlyFlushOutgoingMessages() {
        setupTestEnvAndWaitForConnections();

        AtomicInteger receivedBytes = clientSendMessages("test", 1024);

        clientIOsession.stop(Deadline.of(Duration.ofSeconds(10)));
        assertThat(client.isConnected()).isFalse();
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onShutdown(serverClientIOsession));
        await().untilAtomic(receivedBytes, Matchers.equalTo(1024 * "test".length()));
    }

    @Test
    void testClientStoppingCorrectlyWaitForMessageBeingRead() {
        setupTestEnvAndWaitForConnections();

        AtomicBoolean reading = new AtomicBoolean(false);
        AtomicBoolean readingDone = new AtomicBoolean(false);
        doAnswer(invocationOnMock -> {
            reading.set(true);
            LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(2));
            readingDone.set(true);
            return null;
        }).when(clientIoEventsListener).onRead(any(), any(), anyLong());

        serverClientIOsession.send("Hello world".getBytes());

        await().untilAtomic(reading, Matchers.is(true));

        clientIOsession.stop(Deadline.of(Duration.ofSeconds(10)));

        await().untilAtomic(readingDone, Matchers.is(true));

        assertThat(client.isConnected()).isFalse();
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onShutdown(serverClientIOsession));
    }

    @Test
    void testAttachObjectToSession() {
        setupTestEnvAndWaitForConnections();

        serverClientIOsession.setAttachment("test1");
        clientIOsession.setAttachment("test2");

        String attachement = serverClientIOsession.getAttachment();
        assertThat(attachement).isEqualTo("test1");

        attachement = clientIOsession.getAttachment();
        assertThat(attachement).isEqualTo("test2");
    }

    @Test
    void testTags() {
        setupTestEnvAndWaitForConnections();

        assertThat(clientIOsession.matchesTag(IOSessionTag.of("test1"))).isFalse();
        clientIOsession.addTag(IOSessionTag.of("test1"));
        clientIOsession.addTag(IOSessionTag.of("test2"));
        assertThat(clientIOsession.matchesTag(IOSessionTag.of("test1"))).isTrue();
        assertThat(clientIOsession.matchesTag(IOSessionTag.of("test2"))).isTrue();

        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"))).isTrue();
        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"), IOSessionTag.of("test2"))).isTrue();
        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"), IOSessionTag.of("test3"))).isFalse();
        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"), IOSessionTag.of("test2"), IOSessionTag.of("test3"))).isFalse();

        clientIOsession.removeTag(IOSessionTag.of("test2"));
        assertThat(clientIOsession.matchesTag(IOSessionTag.of("test2"))).isFalse();

        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"))).isTrue();
        assertThat(clientIOsession.matchesTags(IOSessionTag.of("test1"), IOSessionTag.of("test2"))).isFalse();
    }

    @Test
    void testTaskExecution() {
        setupTestEnvAndWaitForConnections();

        BiConsumer<Runnable, Exception> callback = mock(BiConsumer.class);
        AtomicInteger incrementTask = new AtomicInteger();

        serverClientIOsession.processTask(incrementTask::getAndIncrement, callback);
        clientIOsession.processTask(incrementTask::getAndIncrement, callback);

        await().untilAtomic(incrementTask, Matchers.equalTo(2));
        verify(callback, times(2)).accept(any(Runnable.class), isNull());

    }

    @Test
    void testTaskExecutionWithException() {
        setupTestEnvAndWaitForConnections();

        BiConsumer<Runnable, Exception> callback = mock(BiConsumer.class);
        AtomicInteger incrementTask = new AtomicInteger();
        Runnable taskWithError = () -> {
            incrementTask.incrementAndGet();
            throw new IllegalStateException("test error");
        };

        serverClientIOsession.processTask(taskWithError, callback);
        clientIOsession.processTask(taskWithError, callback);

        await().untilAtomic(incrementTask, Matchers.equalTo(2));
        verify(callback, times(2)).accept(any(Runnable.class), isA(IllegalStateException.class));

    }

    @Test
    void testTaskExecutionWithNoCallback() {
        setupTestEnvAndWaitForConnections();

        AtomicInteger incrementTask = new AtomicInteger();
        serverClientIOsession.processTask(incrementTask::getAndIncrement);
        clientIOsession.processTask(incrementTask::getAndIncrement);

        await().untilAtomic(incrementTask, Matchers.equalTo(2));
    }

    @Test
    void testClientWaitsForMessagesFlushing() {
        setupTestEnvAndWaitForConnections();

        AtomicInteger receivedBytes = clientSendMessages("test", 1024);
        boolean flushed = clientIOsession.waitForAllMessagesSent(Deadline.of(Duration.ofSeconds(10)));
        assertThat(flushed).isTrue();
        // waitForAllMessagesSent only says the sender flushed, the peer counting the bytes in is a separate thread and
        // lags behind it on a loaded machine, hence the await here as well as on the second batch below
        await().untilAtomic(receivedBytes, Matchers.equalTo(1024 * "test".length()));

        // a batch big enough to still be in flight when the deadline is evaluated is not something we can enqueue:
        // the IO thread drains the request buffer as fast as we fill it, so it is regularly empty by the time
        // waitForAllMessagesSent looks at it. Holding the IO thread inside a task makes the messages enqueued
        // behind it deterministically pending. Stay below IOSettings.DEFAULT_TASKS_RING_BUFFER_SIZE so send()
        // does not block on a request buffer nothing is consuming.
        CountDownLatch ioThreadBlocked = new CountDownLatch(1);
        CountDownLatch releaseIOThread = new CountDownLatch(1);
        clientIOsession.processTask(() -> {
            ioThreadBlocked.countDown();
            awaitLatch(releaseIOThread);
        });
        awaitLatch(ioThreadBlocked);

        receivedBytes = clientSendMessages("test", 8);
        flushed = clientIOsession.waitForAllMessagesSent(Deadline.of(Duration.ofMillis(50)));
        assertThat(flushed).isFalse();

        releaseIOThread.countDown();
        flushed = clientIOsession.waitForAllMessagesSent(Deadline.of(Duration.ofSeconds(10)));
        assertThat(flushed).isTrue();
        await().untilAtomic(receivedBytes, Matchers.equalTo(8 * "test".length()));
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(ex);
        }
    }

    @Test
    void testSessionPauseAndResume() {
        setupTestEnv(getTestClientBuilder().toBuilder()
                .ioSettings(IOSettings.builder().tasksRingBufferSize(1024).build())
                .build(), getTestServerBuilder());
        startAndWaitForConnections();

        AtomicInteger receivedBytesServer = trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
        AtomicInteger receivedBytesClient = trapReceivedBytesCount(clientIoEventsListener, clientIOsession);

        byte[] msgSrv = "Hello world srv".getBytes();
        byte[] msgClient = "Hello world cli".getBytes();

        for (int i = 0; i < 1024; i++) {
            serverClientIOsession.send(msgSrv);
            clientIOsession.send(msgClient);
        }

        await().untilAtomic(receivedBytesServer, Matchers.equalTo(1024 * msgSrv.length));
        await().untilAtomic(receivedBytesClient, Matchers.equalTo(1024 * msgSrv.length));
        receivedBytesClient.set(0);
        receivedBytesServer.set(0);

        for (int i = 0; i < 1024; i++) {
            if (i == 256) {
                clientIOsession.pause(Deadline.immediate());
            }
            serverClientIOsession.send(msgSrv);
            clientIOsession.send(msgClient);
        }

        int currentReceivedBytesClient = receivedBytesClient.get();

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));

        assertThat(receivedBytesClient.get()).isEqualTo(currentReceivedBytesClient);

        clientIOsession.resume();

        await().untilAtomic(receivedBytesServer, Matchers.equalTo(1024 * msgSrv.length));
        await().untilAtomic(receivedBytesClient, Matchers.equalTo(1024 * msgSrv.length));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testClientSessionIsRejected(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnv(clientBuilder, serverBuilder.toBuilder()
                .remoteSessionsFilter((remoteAddress, remoteCertificates) -> {
                            log.info("Rejecting IOSession for test");
                            return false;
                        }
                ).build());

        server.start();
        client.start();

        await().untilAsserted(() -> verify(serverIoEventsListener).onSessionRejected(any()));
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(any()));
        await().untilAsserted(() -> assertThat(client.isConnected()).isFalse());
    }
}