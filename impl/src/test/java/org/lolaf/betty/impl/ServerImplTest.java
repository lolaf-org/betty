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


import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.*;
import org.lolaf.betty.api.lb.MinIOThreadLoadSessionLoadBalancer;
import org.lolaf.betty.api.settings.IOBufferPoolSettings;
import org.lolaf.betty.api.settings.IOWorkersGroupSettings;
import org.lolaf.betty.api.stats.IOStats;
import org.lolaf.betty.api.stats.IOWorkerStats;
import org.lolaf.ringos.Deadline;

import javax.net.ssl.SSLHandshakeException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Awaitility.with;
import static org.mockito.Mockito.*;


class ServerImplTest extends AbstractTest {

    /**
     * Whether the error is only the peer having closed its socket abortively. Stopping a session on an immediate
     * deadline closes the channel without draining what is still in flight, and on the SSL variant without a TLS
     * close_notify, so the other end can be given a TCP reset rather than an orderly end of stream. Which of the two it
     * observes is up to the kernel's timing, and a reset is ordinary behaviour for an abortive close rather than a
     * failure of the connector under test, so a test asserting an immediate stop raises no error at all on the peer
     * fails at random.
     */
    private static boolean isPeerAbortiveClose(Exception error) {
        return error instanceof SocketException && String.valueOf(error.getMessage()).contains("Connection reset");
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testConnect(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder);

        await().untilAsserted(() -> verify(serverIoEventsListener).onConnected(serverClientIOsession));
        await().untilAsserted(() -> verify(clientIoEventsListener).onConnected(clientIOsession));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testDisconnectCallsEventsListener(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnvAndWaitForConnections(clientBuilder, serverBuilder);

        assertThat(serverClientIOsession.isStarted()).isTrue();
        assertThat(clientIOsession.isStarted()).isTrue();

        server.stop(Deadline.immediate());

        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));
        await().untilAsserted(() -> verify(clientIoEventsListener).onDisconnected(clientIOsession));

        assertThat(serverClientIOsession.isStarted()).isFalse();
        assertThat(clientIOsession.isStarted()).isFalse();
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSharedIOWorkGroup(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        IOWorkersGroup shared = IOWorkersGroupSettings.builder().id("shared").build().newInstance();
        clientBuilder = clientBuilder.toBuilder().ioWorkersGroup(shared).build();
        serverBuilder = serverBuilder.toBuilder().ioWorkersGroup(shared)
                .acceptorIoWorkerGroup(IOWorkersGroupSettings.builder().id("acceptor").build().newInstance()).build();

        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from client";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(message.getBytes());

        verifyReceivedMessage(receivedBytes, message);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSharedIOBufferPool(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        serverBuilder = serverBuilder.toBuilder().ioSessionsSharedWriteIoBufferPoolSettings(IOBufferPoolSettings.builder()
                .zone(IOBufferPoolSettings.IOBufferPoolZone.builder().build()).build()).build();

        testConnect(clientBuilder, serverBuilder);

        IOBufferPool pool = SharedIOBufferPoolInstanceRegistry.getInstance().getPool(serverBuilder.getId());
        assertThat(pool).isNotNull();

        String message = "hello world from server";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        serverClientIOsession.send(message.getBytes());

        verifyReceivedMessage(receivedBytes, message);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testIOWorkerLoad(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        IOWorkersGroup clientGroup = IOWorkersGroupSettings.builder()
                .id("client")
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(2).build())
                .ioWorkerLoadBalancer(MinIOThreadLoadSessionLoadBalancer.getInstance())
                .build().newInstance();
        IOWorkersGroup serverGroup = IOWorkersGroupSettings.builder()
                .id("server")
                .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(3).build())
                .ioWorkerLoadBalancer(MinIOThreadLoadSessionLoadBalancer.getInstance())
                .build().newInstance();

        clientBuilder = clientBuilder.toBuilder().ioWorkersGroup(clientGroup).build();
        serverBuilder = serverBuilder.toBuilder().ioWorkersGroup(serverGroup).build();

        setupTestEnv(clientBuilder, serverBuilder);

        server.start();

        assertThat(serverGroup.getIOWorkers()).allMatch(w -> w.getLoad() == 0D);

        List<Client> clients = new ArrayList<>();
        List<TestIOEventsListener> listeners = new ArrayList<>();
        int clientSessionsCount = 4;
        for (int i = 0; i < clientSessionsCount; i++) {
            getTestClient(clientBuilder, listeners, clients, i);
        }

        clients.forEach(c -> {
            c.start();
            // we must wait to be connected because the session will be bound to the IOThread only when connected
            await().until(c::isConnected);
        });

        await().untilAsserted(() -> assertThat(clientGroup.getIOWorkers()).allMatch(w -> w.getLoad() > 0D));

        for (int i = 0; i < 10; i++) {
            String message = "hello world from server";
            server.broadcast(ByteBuffer.wrap(message.getBytes()));
            awaitForReceivedMessage(listeners, message, clients.size());
        }

        await().untilAsserted(() -> assertThat(serverGroup.getIOWorkers()).allMatch(w -> w.getLoad() > 0D));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testIOStats(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        IOStats serverStats = mock(IOStats.class);
        IOStats clientStats = mock(IOStats.class);

        testConnect(clientBuilder.toBuilder()
                        .ioStatsProvider(s -> clientStats)
                        .build(),
                serverBuilder.toBuilder()
                        .ioStatsProvider(s -> serverStats)
                        .build());

        String message = "hello world from client";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);
        verify(serverStats, never()).onSocketRead(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats, never()).onSocketWrite(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats, never()).onMessageSent(any(IOSession.class), any(ByteBuffer.class), any(), anyLong());

        client.enableIOStats(true);
        server.enableIOStats(true);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);
        verify(serverStats).onSocketRead(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats).onSocketWrite(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats).onMessageSent(any(IOSession.class), any(ByteBuffer.class), any(), anyLong());

        client.enableIOStats(false);
        server.enableIOStats(false);
        reset(serverStats, clientStats);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);
        verify(serverStats, never()).onSocketRead(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats, never()).onSocketWrite(any(IOSession.class), anyInt(), anyLong());
        verify(clientStats, never()).onMessageSent(any(IOSession.class), any(ByteBuffer.class), any(), anyLong());
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testIOWorkerStats(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        IOWorkerStats serverStats = mock(IOWorkerStats.class);
        when(serverStats.getStatsResolution()).thenReturn(Duration.ofSeconds(1));
        IOWorkerStats clientStats = mock(IOWorkerStats.class);
        when(clientStats.getStatsResolution()).thenReturn(Duration.ofSeconds(1));

        testConnect(clientBuilder.toBuilder()
                        .ioWorkersGroup(IOWorkersGroupSettings.builder().ioWorkerStatisticsProvider(ioWorker -> clientStats).build().newInstance())
                        .build(),
                serverBuilder.toBuilder()
                        .ioWorkersGroup(IOWorkersGroupSettings.builder().ioWorkerStatisticsProvider(ioWorker -> serverStats).build().newInstance())
                        .build());

        String message = "hello world from client";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);
        verify(serverStats, never()).onReadCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());
        verify(clientStats, never()).onWriteCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());

        client.enableIOWorkerStats(true);
        server.enableIOWorkerStats(true);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);
        await().untilAsserted(() -> {
            verify(serverStats).onReadCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());
            verify(clientStats).onWriteCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());
        });

        client.enableIOWorkerStats(false);
        server.enableIOWorkerStats(false);
        reset(serverStats, clientStats);

        clientIOsession.send(message.getBytes());
        verifyReceivedMessage(receivedBytes, message);

        verify(serverStats, never()).onReadCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());
        verify(clientStats, never()).onWriteCPUTimeTaken(any(IOWorker.class), any(IOSession.class), anyLong());
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testReceivingMessage(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from client";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(message.getBytes());

        verifyReceivedMessage(receivedBytes, message);
        verify(serverIoEventsListener).onRead(eq(serverClientIOsession), any(ByteBuffer.class), longThat(localReceiveTime -> localReceiveTime == 0));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testReceivingMessageWithReceiveTimeEnabled(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder.toBuilder()
                .ioSettings(serverBuilder.getIoSettings().toBuilder()
                        .trackReceiveTime(true)
                        .build())
                .build());
        String message = "hello world from client";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(message.getBytes());

        verifyReceivedMessage(receivedBytes, message);
        verify(serverIoEventsListener).onRead(eq(serverClientIOsession), any(ByteBuffer.class), longThat(localReceiveTime -> localReceiveTime > 0));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testBroadcastMessage(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnv(clientBuilder, serverBuilder);

        AtomicInteger sessionId = new AtomicInteger();
        doNothing().when(serverIoEventsListener).onConnected(argThat(ioSession -> {
            ioSession.addTag(getIoSessionTag(sessionId.get()));
            ioSession.addTag(IOSessionTag.of("test-tag"));
            ioSession.setAttachment(sessionId.getAndIncrement());
            return true;
        }));

        server.start();

        List<Client> clients = new ArrayList<>();
        List<TestIOEventsListener> listeners = new ArrayList<>();
        int clientSessionsCount = 16;
        for (int i = 0; i < clientSessionsCount; i++) {
            getTestClient(clientBuilder, listeners, clients, i);
        }
        clients.forEach(Client::start);

        await().untilAsserted(() -> assertThat(server.getIOSessions()).hasSize(clientSessionsCount));

        String message = "hello world";
        int sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()));

        assertThat(sentCount).isEqualTo(clientSessionsCount);
        awaitForReceivedMessage(listeners, message, clientSessionsCount);

        sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()), getIoSessionTag(0), IOSessionTag.of("test-tag"));

        assertThat(sentCount).isEqualTo(1);
        awaitForReceivedMessage(listeners, message, 1);

        sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()), getIoSessionTag(10));

        assertThat(sentCount).isEqualTo(1);
        awaitForReceivedMessage(listeners, message, 1);

        sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()), iosession -> {
            int sid = iosession.getAttachment();
            return sid % 2 == 0;
        });

        assertThat(sentCount).isEqualTo(clientSessionsCount / 2);
        awaitForReceivedMessage(listeners, message, clientSessionsCount / 2);

        sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()), s -> false);

        assertThat(sentCount).isZero();

        sentCount = server.broadcast(ByteBuffer.wrap(message.getBytes()), IOSessionTag.of("unknown-tag"));

        assertThat(sentCount).isZero();

        clients.forEach(c -> c.stop(Deadline.immediate()));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testMultipleClientWithMultipleIOWorkers(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        server = serverBuilder.toBuilder()
                .ioWorkersGroup(IOWorkersGroupSettings.builder()
                        .ioThreadGroup(IOWorkersGroupSettings.IOThreadGroup.builder().ioThreadCount(5).build())
                        .build().newInstance()).build().newInstance().start();

        List<Client> clients = new ArrayList<>();
        List<TestIOEventsListener> listeners = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            getTestClient(clientBuilder, listeners, clients, i);
        }
        clients.forEach(Client::start);

        listeners.forEach(this::waitForConnection);

        await().untilAsserted(() -> clients.forEach(c -> assertThat(c.isConnected()).isTrue()));
        await().untilAsserted(() -> assertThat(server.getIOSessions()).hasSize(64));

        clients.forEach(c -> c.stop(Deadline.immediate()));

    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testMultipleSendingMessageWithSmallRingBuffer(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        TestIOEventsListener testServerListener = new TestIOEventsListener();
        TestIOEventsListener testClientListener = new TestIOEventsListener();

        setupTestEnvAndWaitForConnections(clientBuilder.toBuilder()
                        .ioEventsListener(testClientListener)
                        .ioSettings(clientBuilder.getIoSettings().toBuilder()
                                .tasksRingBufferSize(1024)
                                .writeIoBufferPoolSettings(clientBuilder.getIoSettings().getWriteIoBufferPoolSettings().toBuilder()
                                        .clearZones()
                                        .zone(IOBufferPoolSettings.IOBufferPoolZone.builder()
                                                .poolSize(1024)
                                                .build())
                                        .build())
                                .build())
                        .build(),
                serverBuilder.toBuilder()
                        .ioEventsListener(testServerListener)
                        .build());

        testClientListener.reset();
        testServerListener.reset();

        int messagesCount = 1024 * 1024;
        byte[] message = "Hello world".getBytes();
        for (int i = 0; i < messagesCount; i++) {
            clientIOsession.send(message);
        }

        int totalBytes = messagesCount * message.length;
        with().pollDelay(Duration.ofMillis(2)).untilAtomic(testClientListener.getWrittenBytes(), Matchers.equalTo(totalBytes));
        with().pollDelay(Duration.ofMillis(2)).untilAtomic(testServerListener.getReadenBytes(), Matchers.equalTo(totalBytes));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testServerDisconnectsSession(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        serverClientIOsession.stop(Deadline.immediate());

        await().until(() -> !client.isConnected());

        verify(serverIoEventsListener).onShutdown(serverClientIOsession);
        verify(serverIoEventsListener).onDisconnected(serverClientIOsession);
        verify(serverIoEventsListener, never()).onError(any(), any());
        // the side being closed on may be handed a reset instead of a clean end of stream, see isPeerAbortiveClose:
        // anything else reaching the client is still a failure
        verify(clientIoEventsListener, never()).onError(any(), argThat(error -> !isPeerAbortiveClose(error)));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingMessage(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from server";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        serverClientIOsession.send(message.getBytes());

        verifyReceivedMessage(receivedBytes, message);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingMessageWithCallbackAndNoContext(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from server";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        IOWriter.MessageSentCallback callback = mock(IOWriter.MessageSentCallback.class);

        ByteBuffer bb = ByteBuffer.wrap(message.getBytes());
        bb.position(bb.capacity());

        serverClientIOsession.send(bb, null, callback, false);

        verifyReceivedMessage(receivedBytes, message);

        verify(callback).onMessageWriteCallback(any(ByteBuffer.class), isNull(), isNull());
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingMessageWithCallback(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from server";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        IOWriter.MessageSentCallback<String> callback = mock(IOWriter.MessageSentCallback.class);

        ByteBuffer bb = ByteBuffer.wrap(message.getBytes());
        bb.position(bb.capacity());

        serverClientIOsession.send(bb, "testContext", callback, false);

        verifyReceivedMessage(receivedBytes, message);

        verify(callback).onMessageWriteCallback(any(ByteBuffer.class), isNull(), eq("testContext"));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingMessageWithFuture(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        String message = "hello world from server";

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        ByteBuffer bb = ByteBuffer.wrap(message.getBytes());
        bb.position(bb.capacity());

        CompletableFuture<String> future = serverClientIOsession.send(bb, "testString", false);
        String ctx = null;
        try {
            ctx = future.get();
        } catch (Exception e) {
            Assertions.fail(e);
        }
        assertThat(future.isDone()).isTrue();
        assertThat(ctx).isEqualTo("testString");

        verifyReceivedMessage(receivedBytes, message);
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingMessageWithByteBufferBuilder(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);

        String message = "hello world from server";

        IOWriter.ByteBufferBuilder byteBufferBuilder = new IOWriter.ByteBufferBuilder() {
            @Override
            public ByteBuffer build() {
                ByteBuffer bb = ByteBuffer.wrap(message.getBytes());
                bb.position(bb.capacity());
                return bb.position(bb.capacity());
            }

            @Override
            public boolean isPooledByteBuffer() {
                return false;
            }

            @Override
            public int getEstimatedByteBufferSize() {
                return message.length();
            }
        };

        AtomicReference<byte[]> receivedBytes = trapReceivedMessage(clientIoEventsListener, clientIOsession);

        IOWriter.MessageSentCallback<String> callback = mock(IOWriter.MessageSentCallback.class);

        serverClientIOsession.send(byteBufferBuilder, "testContext", callback);

        verifyReceivedMessage(receivedBytes, message);

        verify(callback).onMessageWriteCallback(any(ByteBuffer.class), isNull(), eq("testContext"));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingBigMessageOneWay(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        byte[] message = new byte[1024 * 1024 * 4];
        AtomicInteger receivedBytesCount = trapReceivedBytesCount(clientIoEventsListener, clientIOsession);

        serverClientIOsession.send(message);

        await().untilAtomic(receivedBytesCount, Matchers.equalTo(message.length));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testSendingBigMessageTwoWays(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        AtomicInteger receivedClientBytesCount = new AtomicInteger();
        AtomicInteger receivedServerBytesCount = new AtomicInteger();
        // This one has failed intermittently under the full reactor, and only there, with the client never seeing the
        // echo come back. The counters alone cannot say whether the payload failed to leave the client, failed to
        // reach the server, or died on the way back, so everything the IO layer reports on either side is collected
        // and quoted in the failure message - the next occurrence should be diagnosable from the report alone.
        List<String> ioEvents = new CopyOnWriteArrayList<>();

        setupTestEnv(clientBuilder.toBuilder()
                        .ioEventsListener(new DiagnosingIOEventsListener("client", ioEvents) {
                            @Override
                            public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
                                receivedClientBytesCount.addAndGet(message.remaining());
                                message.position(message.limit());
                            }
                        }).build(),
                serverBuilder.toBuilder()
                        .ioEventsListener(new DiagnosingIOEventsListener("server", ioEvents) {
                            @Override
                            public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
                                receivedServerBytesCount.addAndGet(message.remaining());
                                session.send(session.borrow(message.remaining()).put(message), true);
                            }
                        }).build());

        server.start();
        client.start();

        await().untilAsserted(() -> assertThat(client.isConnected()).isTrue());

        byte[] message = new byte[1024 * 1024 * 4];
        client.getIOSession().send(message);

        // the server is asserted first because the echo cannot precede it: failing on the client's counter while the
        // server's is untold says only that something went wrong somewhere, which is how the flake used to report
        await().untilAsserted(() -> {
            assertThat(receivedServerBytesCount)
                    .as("bytes the server received (client received %s, IO events %s)", receivedClientBytesCount, ioEvents)
                    .hasValue(message.length);
            assertThat(receivedClientBytesCount)
                    .as("bytes the client received back (server received %s, IO events %s)", receivedServerBytesCount, ioEvents)
                    .hasValue(message.length);
        });
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testClientDisconnect(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);

        client.stop(Deadline.immediate());

        await().untilAsserted(() -> verify(clientIoEventsListener).onDisconnected(clientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testStopStart(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        // clearInvocations rather than reset: reset would drop the stubbing too, and a listener that stops answering
        // isReadyToConnect() is a client that never dials again
        clearInvocations(clientIoEventsListener);
        server.stop(Deadline.immediate());

        await().untilAsserted(() -> verify(clientIoEventsListener).onDisconnected(clientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));

        server.start();

        await().untilAsserted(() -> verify(clientIoEventsListener).onConnected(any()));
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(any()));
    }

    @ParameterizedTest
    @MethodSource("getTestParams")
    void testWriteFailure(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        testConnect(clientBuilder, serverBuilder);
        // clearInvocations rather than reset: reset would drop the stubbing too, and a listener that stops answering
        // isReadyToConnect() is a client that never dials again
        clearInvocations(clientIoEventsListener);
        server.stop(Deadline.immediate());

        await().untilAsserted(() -> verify(clientIoEventsListener).onDisconnected(clientIOsession));
        await().untilAsserted(() -> verify(serverIoEventsListener).onDisconnected(serverClientIOsession));

        clientIOsession.send("test error".getBytes());

        await().untilAsserted(() -> verify(clientIoEventsListener).onWriteFailure(eq(clientIOsession), any()));
    }

    private void getTestClient(ClientBuilder clientBuilder, List<TestIOEventsListener> listeners, List<Client> clients, int i) {
        TestIOEventsListener testListener = new TestIOEventsListener();
        listeners.add(testListener);
        // no connectAddress here: the one carried by clientBuilder already points at this test's server, and
        // connectAddress is @Singular so setting it again would append a second, wrong address
        clients.add(clientBuilder.toBuilder()
                .id("test-client-" + i)
                .ioEventsListener(testListener)
                .build().newInstance());
    }

    private IOSessionTag getIoSessionTag(int id) {
        return IOSessionTag.of("" + id);
    }

    private void awaitForReceivedMessage(List<TestIOEventsListener> listeners, String message, int expectedClientSessionsCount) {
        // this fans a broadcast out to several sessions sharing a handful of IO threads, over TLS in the SSL variant, so
        // the last session can take a while to be served on a busy machine: it relies on the timeout AbstractTest sets
        await().untilAsserted(() -> {
            long receivedCount = listeners.stream().filter(l -> l.getReadenBytes().get() == message.length()).count();
            assertThat(receivedCount).isEqualTo(expectedClientSessionsCount);
        });
        listeners.forEach(l -> l.getReadenBytes().set(0));
    }

    /**
     * Records everything the IO layer reports other than reads into a shared list, so that a test failing on a byte
     * count can say what happened around it rather than only that the count was wrong. Subclasses supply
     * {@link #onRead}, which is the only callback with no default.
     */
    private abstract static class DiagnosingIOEventsListener implements IOEventsListener {

        private final String side;
        private final List<String> ioEvents;

        private DiagnosingIOEventsListener(String side, List<String> ioEvents) {
            this.side = side;
            this.ioEvents = ioEvents;
        }

        private void recordEvent(String event) {
            ioEvents.add(side + " " + event);
        }

        @Override
        public void onWriteFailure(IOSession session, ByteBuffer message) {
            recordEvent("onWriteFailure of " + message.remaining() + " bytes");
        }

        @Override
        public void onDisconnected(IOSession session) {
            recordEvent("onDisconnected");
        }

        @Override
        public void onShutdown(IOSession session) {
            recordEvent("onShutdown");
        }

        @Override
        public void onError(IOSession session, Exception error) {
            recordEvent("onError " + error);
        }

        @Override
        public void onFailedSSLHandshake(IOSession session, SSLHandshakeException exception) {
            recordEvent("onFailedSSLHandshake " + exception);
        }

        @Override
        public void onSessionRejected(IOSession session) {
            recordEvent("onSessionRejected");
        }

        @Override
        public void onWatermarkEvent(IOSession session, boolean highWatermarkReached, long bytesLeftToWrite) {
            recordEvent("onWatermarkEvent high=" + highWatermarkReached + " bytesLeftToWrite=" + bytesLeftToWrite);
        }
    }
}