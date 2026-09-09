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
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.provider.Arguments;
import org.lolaf.betty.api.Client;
import org.lolaf.betty.api.ClientBuilder;
import org.lolaf.betty.api.Server;
import org.lolaf.betty.api.ServerBuilder;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.SSLSettings;
import org.lolaf.betty.api.settings.ServerSSLSettings;
import org.mockito.ArgumentCaptor;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.mockito.Mockito.*;

public abstract class AbstractTest {

    private static final Duration DEFAULT_AWAIT_TIMEOUT = Duration.ofSeconds(30);
    IOEventsListener serverIoEventsListener;
    IOEventsListener clientIoEventsListener;
    Server server;
    Client client;
    IOSession clientIOsession;
    IOSession serverClientIOsession;
    ScheduledExecutorService scheduledExecutorService;
    /**
     * Port this test binds. Allocated per test instance - JUnit creates one instance per test method - so that test
     * classes and test methods can run concurrently.
     */
    int port;

    @BeforeAll
    static void raiseAwaitilityTimeout() {
        Awaitility.setDefaultTimeout(DEFAULT_AWAIT_TIMEOUT);
    }

    /**
     * JUnit resolves the arguments of a {@code @ParameterizedTest} before the test instance exists, so this cannot use
     * the per-instance {@link #port}: each pair of builders takes a port of its own instead. The factory is invoked once
     * per test method, so no two invocations - in this class or in another one running at the same time - ever bind the
     * same port.
     */
    static Stream<Arguments> getTestParams() {
        int plainPort = TestPorts.findFree();
        int sslPort = TestPorts.findFree();
        return Stream.of(
                Arguments.of(getTestClientBuilder(plainPort), getTestServerBuilder(plainPort)),
                Arguments.of(getTestClientBuilder(sslPort).toBuilder().SSLSettings(SSLSettings.builder()
                                .sslContext(getClientSSLContext()).build()).build()
                        , getTestServerBuilder(sslPort).toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build()).build())
        );
    }

    static SSLContext getClientSSLContext() {
        return createSSLContext("clientkeystore.p12", "clienttruststore.jks");
    }

    static SSLContext getServerSSLContext() {
        return createSSLContext("serverkeystore.p12", "servertruststore.jks");
    }

    private static SSLContext createSSLContext(String keyStoreName, String trustStorename) {
        try {
            char[] passphrase = "password".toCharArray();
            SSLContext ctx = SSLContext.getInstance("TLSv1.3");
            KeyStore ks = KeyStore.getInstance("PKCS12");
            InputStream in = ServerImplTest.class.getClassLoader().getResourceAsStream(keyStoreName);
            if (in == null) {
                throw new IllegalStateException("Cannot find " + keyStoreName);
            }
            ks.load(in, passphrase);
            in.close();

            KeyStore ts = KeyStore.getInstance("JKS");
            in = ServerImplTest.class.getClassLoader().getResourceAsStream(trustStorename);
            if (in == null) {
                throw new IllegalStateException("Cannot find " + keyStoreName);
            }
            ts.load(in, passphrase);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, passphrase);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ts);
            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
            return ctx;
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    static ClientBuilder getTestClientBuilder(int port) {
        return ClientBuilder.builder()
                .id("test-client-" + port)
                .connectAddress(new InetSocketAddress("localhost", port))
                .ioEventsListener(getIOEventsListenerMock())
                .scheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
                .build();
    }

    static ServerBuilder getTestServerBuilder(int port) {
        return ServerBuilder.builder()
                .id("test-server-" + port)
                .ioEventsListener(getIOEventsListenerMock())
                .bindAddress(new InetSocketAddress("localhost", port))
                .build();
    }

    private static IOEventsListener getIOEventsListenerMock() {
        IOEventsListener mock = mock(IOEventsListener.class);
        // a mock answers false where the interface defaults to true, which on this one means "do not connect at all"
        when(mock.isReadyToConnect(any())).thenReturn(true);
        doAnswer(invocationOnMock -> {
            ByteBuffer byteBuffer = invocationOnMock.getArgument(1);
            byteBuffer.position(byteBuffer.limit());
            return null;
        }).when(mock).onRead(any(IOSession.class), any(ByteBuffer.class), anyLong());
        return mock;
    }

    ClientBuilder getTestClientBuilder() {
        return getTestClientBuilder(port);
    }

    ServerBuilder getTestServerBuilder() {
        return getTestServerBuilder(port);
    }

    @BeforeEach
    public void before() {
        port = TestPorts.findFree();
        serverIoEventsListener = getIOEventsListenerMock();
        clientIoEventsListener = getIOEventsListenerMock();
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        server = getTestServerBuilder().newInstance();
        client = getTestClientBuilder().newInstance();
    }

    @AfterEach
    public void after() {
        if (client != null) {
            client.stop(org.lolaf.ringos.Deadline.immediate());
        }
        if (server != null) {
            server.stop(org.lolaf.ringos.Deadline.immediate());
        }
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
    }

    void setupTestEnv() {
        setupTestEnv(getTestClientBuilder(), getTestServerBuilder());
    }

    void setupTestEnvAndWaitForConnections() {
        setupTestEnv();
        startAndWaitForConnections();
    }

    void startAndWaitForConnections() {
        client.start();
        server.start();
        serverClientIOsession = waitForConnection(serverIoEventsListener);
        clientIOsession = waitForConnection(clientIoEventsListener);
    }

    void setupTestEnvAndWaitForConnections(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        setupTestEnv(clientBuilder, serverBuilder);
        startAndWaitForConnections();
    }

    void setupTestEnv(ClientBuilder clientBuilder, ServerBuilder serverBuilder) {
        server = serverBuilder.newInstance();
        client = clientBuilder.newInstance();
        serverIoEventsListener = serverBuilder.getIoEventsListener();
        clientIoEventsListener = clientBuilder.getIoEventsListener();
        scheduledExecutorService = clientBuilder.getScheduledExecutorService();
    }

    AtomicReference<byte[]> trapReceivedMessage(IOEventsListener ioEventsListener, IOSession ioSession) {
        AtomicReference<byte[]> receivedBytes = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            ByteBuffer byteBuffer = invocationOnMock.getArgument(1);
            byte[] received = new byte[byteBuffer.remaining()];
            byteBuffer.get(received);
            receivedBytes.set(received);
            byteBuffer.position(byteBuffer.limit());
            return null;
        }).when(ioEventsListener).onRead(eq(ioSession), any(ByteBuffer.class), anyLong());
        return receivedBytes;
    }

    AtomicInteger trapReceivedBytesCount(IOEventsListener ioEventsListener, IOSession ioSession) {
        AtomicInteger receivedBytesCount = new AtomicInteger();
        doAnswer(invocationOnMock -> {
            ByteBuffer byteBuffer = invocationOnMock.getArgument(1);
            receivedBytesCount.getAndAdd(byteBuffer.remaining());
            byteBuffer.position(byteBuffer.limit());
            return null;
        }).when(ioEventsListener).onRead(eq(ioSession), any(ByteBuffer.class), anyLong());
        return receivedBytesCount;
    }

    void verifyReceivedMessage(AtomicReference<byte[]> receivedBytes, String message) {
        verifyReceivedMessage(receivedBytes, message.getBytes());
    }

    void verifyReceivedMessage(AtomicReference<byte[]> receivedBytes, byte[] message) {
        Awaitility.await().untilAsserted(() ->
                Assertions.assertThat(receivedBytes).hasValue(message));
    }

    IOSession waitForConnection(IOEventsListener ioEventsListener) {
        AtomicReference<IOSession> connectedSession = new AtomicReference<>();
        Awaitility.await().untilAsserted(() -> {
            if (ioEventsListener instanceof TestIOEventsListener) {
                TestIOEventsListener l = (TestIOEventsListener) ioEventsListener;
                Assertions.assertThat(l.isConnected()).isTrue();
                connectedSession.set(l.getSession());
            } else {
                ArgumentCaptor<IOSession> ioSessionCaptor = ArgumentCaptor.forClass(IOSession.class);
                verify(ioEventsListener).onConnected(ioSessionCaptor.capture());
                connectedSession.set(ioSessionCaptor.getValue());
            }
        });
        return connectedSession.get();
    }

    @Getter
    static class TestIOEventsListener implements IOEventsListener {

        private final AtomicInteger readenBytes = new AtomicInteger();
        private final AtomicInteger writtenBytes = new AtomicInteger();
        private IOSession session;

        public void reset() {
            readenBytes.set(0);
            writtenBytes.set(0);
        }

        public boolean isConnected() {
            return session != null;
        }

        @Override
        public void onConnected(IOSession session) {
            this.session = session;
        }

        @Override
        public void onDisconnected(IOSession session) {
            this.session = null;
        }

        @Override
        public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
            this.readenBytes.getAndAdd(message.remaining());
            message.position(message.limit());
        }

        @Override
        public void onWrite(IOSession session, ByteBuffer message) {
            this.writtenBytes.getAndAdd(message.limit());
        }
    }
}