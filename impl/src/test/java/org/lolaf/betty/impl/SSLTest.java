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

import org.lolaf.ringos.Deadline;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.io.IOWorker;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.settings.SSLSettings;
import org.lolaf.betty.api.settings.ServerSSLSettings;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLHandshakeException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

class SSLTest extends AbstractTest {

    /**
     * How many sessions the server's workers hold, counting one whose handshake has not finished - which is the point
     * here, such a session being invisible to every listener callback until it either completes or fails.
     */
    private int registeredSessionsCount() {
        return ((ServerImpl) server).getIoWorkersGroup().getIOWorkers().stream()
                .mapToInt(IOWorker::getRegisteredSessionsCount)
                .sum();
    }

    @Test
    void testSessionCloseIsQuick() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build())
                        .build());

        long start = System.currentTimeMillis();
        clientIOsession.stop(Deadline.unlimited());
        long stop = System.currentTimeMillis();
        assertThat(stop - start).isLessThan(50);
    }

    @Test
    void testConnectWithServerCertificate() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build())
                        .build());

        await().untilAsserted(() ->
                verify(clientIoEventsListener).onSSLHandshake(eq(clientIOsession),
                        assetRemoteCertificate("CN=testserver, OU=test, O=test, L=test, ST=test, C=CH")));
        await().untilAsserted(() ->
                verify(serverIoEventsListener).onSSLHandshake(eq(serverClientIOsession),
                        isNull()));
    }

    @Test
    void testConnectWithRequiredClientCertificate() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .needClientAuth(true)
                                .wantClientAuth(true)
                                .sslContext(getServerSSLContext()).build())
                        .build());

        await().untilAsserted(() ->
                verify(clientIoEventsListener).onSSLHandshake(eq(clientIOsession),
                        assetRemoteCertificate("CN=testserver, OU=test, O=test, L=test, ST=test, C=CH")));
        await().untilAsserted(() ->
                verify(serverIoEventsListener).onSSLHandshake(eq(serverClientIOsession),
                        assetRemoteCertificate("CN=testclient, OU=test, O=test, L=test, ST=test, C=CH")));
    }


    @Test
    void testConnectionWithFailedSSLHandshake() throws Exception {
        setupTestEnv(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(SSLContext.getDefault()).build()).build(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(SSLContext.getDefault()).build()).build());

        server.start();
        client.start();

        // atLeastOnce(), not the implicit times(1): a client whose handshake fails keeps reconnecting every
        // ClientBuilder.connectionRetry, and every attempt fails the same way, so the count climbs for as long as the
        // test runs. Waiting for it to be exactly one is waiting for a condition that stops holding after a second and
        // never holds again - any hiccup between client.start() and the first poll turns the test into a guaranteed
        // timeout rather than a slower pass.
        await().untilAsserted(() ->
                verify(clientIoEventsListener, atLeastOnce()).onFailedSSLHandshake(any(IOSession.class), assertArg(e ->
                        assertThat(e).isInstanceOf(SSLHandshakeException.class))));

        await().untilAsserted(() ->
                verify(serverIoEventsListener, atLeastOnce()).onFailedSSLHandshake(any(IOSession.class), assertArg(e ->
                        assertThat(e).isInstanceOf(SSLHandshakeException.class))));
    }

    /**
     * A peer that opens a socket and then says nothing must not cost the worker anything, and above all must not stop
     * it serving everyone else.
     * <p>
     * This is the case that used to wedge betty outright: the handshake was a blocking loop reading a non-blocking
     * channel, and {@code read} answering 0 - which is what a silent peer produces - sent it straight back round to
     * read again. One such peer pinned the IO worker at 100% CPU, starved every session sharing it, and left the
     * engine unable to stop, because a busy loop cannot be interrupted.
     */
    @Test
    void testSilentPeerDoesNotWedgeTheWorker() throws Exception {
        setupTestEnv(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(getServerSSLContext()).build()).build());
        server.start();

        // a bare socket that completes the TCP connection and then sends not one byte
        try (Socket silent = new Socket("localhost", port)) {
            assertThat(silent.isConnected()).isTrue();

            // the worker carries on regardless: a real client still handshakes and its traffic still arrives
            client.start();
            serverClientIOsession = waitForConnection(serverIoEventsListener);
            clientIOsession = waitForConnection(clientIoEventsListener);

            AtomicInteger receivedBytes = super.trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);
            clientIOsession.send(new byte[512]);
            await().untilAtomic(receivedBytes, Matchers.equalTo(512));
        }
    }

    /**
     * And the silent peer is eventually let go of, rather than holding its socket and buffers for ever. Nothing else
     * would notice it: sending nothing, it produces no readiness events at all.
     */
    @Test
    void testSilentPeerIsClosedOnTheHandshakeDeadline() throws Exception {
        setupTestEnv(getTestClientBuilder(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(getServerSSLContext())
                        .handshakeTimeout(Duration.ofSeconds(1))
                        .build()).build());
        server.start();

        try (Socket silent = new Socket("localhost", port)) {
            assertThat(silent.isConnected()).isTrue();

            await().untilAsserted(() ->
                    verify(serverIoEventsListener, atLeastOnce()).onFailedSSLHandshake(any(IOSession.class), assertArg(e ->
                            assertThat(e).isInstanceOf(SSLHandshakeException.class))));
        }
    }

    /**
     * A peer speaking something that is not TLS at all - the shape of a plaintext client pointed at a TLS port - is
     * refused rather than spun on.
     */
    @Test
    void testPlaintextPeerFailsTheHandshake() throws Exception {
        setupTestEnv(getTestClientBuilder(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(getServerSSLContext()).build()).build());
        server.start();

        try (Socket plaintext = new Socket("localhost", port)) {
            plaintext.getOutputStream().write("8=FIX.4.49=4235=A".getBytes(StandardCharsets.US_ASCII));
            plaintext.getOutputStream().flush();

            await().untilAsserted(() ->
                    verify(serverIoEventsListener, atLeastOnce()).onFailedSSLHandshake(any(IOSession.class), assertArg(e ->
                            assertThat(e).isInstanceOf(SSLHandshakeException.class))));
        }
    }

    /**
     * Stopping while a handshake is outstanding must return, and quickly. It used to join a worker thread that could
     * never exit, on a deadline that is a thousand years when unlimited.
     */
    @Test
    void testStopReturnsWhileAHandshakeIsPending() throws Exception {
        setupTestEnv(getTestClientBuilder(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(getServerSSLContext()).build()).build());
        server.start();

        try (Socket silent = new Socket("localhost", port)) {
            assertThat(silent.isConnected()).isTrue();
            // wait until the server really holds the half-open session, so that stop() below is stopping a worker with
            // a handshake outstanding rather than an idle one
            await().untilAsserted(() -> assertThat(registeredSessionsCount()).isPositive());

            long start = System.currentTimeMillis();
            server.stop(Deadline.of(Duration.ofSeconds(5)));
            assertThat(System.currentTimeMillis() - start)
                    .as("stopping must not wait on a handshake that will never finish")
                    .isLessThan(5_000);
            server = null; // already stopped, keep the teardown from stopping it twice
        }
    }

    @Test
    void testSendingMessageWithReadBufferSizeSmallerMessageSize() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build())
                        .ioSettings(IOSettings.builder().readBufferSize(1024).build())
                        .build());

        AtomicInteger receivedBytes = super.trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(new byte[1024 * 1024 * 10]);

        await().untilAtomic(receivedBytes, Matchers.equalTo(1024 * 1024 * 10));
    }

    @Test
    void testSendingMessageWithReadBufferLargerThanPayload() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build())
                        .ioSettings(IOSettings.builder().readBufferSize(1024).build())
                        .build());

        AtomicInteger receivedBytes = super.trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(new byte[512]);

        await().untilAtomic(receivedBytes, Matchers.equalTo(512));

        receivedBytes.set(0);

        for (int i = 0; i < 512; i++) {
            clientIOsession.send(new byte[1]);
        }

        await().untilAtomic(receivedBytes, Matchers.equalTo(512));
    }

    @Test
    void testCleanSSLSessionClose() {
        setupTestEnvAndWaitForConnections(getTestClientBuilder().toBuilder().SSLSettings(SSLSettings.builder()
                        .sslContext(getClientSSLContext()).build()).build()
                , getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                                .sslContext(getServerSSLContext()).build())
                        .ioSettings(IOSettings.builder().readBufferSize(1024).build())
                        .build());

        AtomicInteger receivedBytes = super.trapReceivedBytesCount(serverIoEventsListener, serverClientIOsession);

        clientIOsession.send(new byte[512]);

        await().untilAtomic(receivedBytes, Matchers.equalTo(512));

        client.stop(Deadline.immediate());

        await().untilAsserted(() ->
                verify(clientIoEventsListener).onSSLSessionEnd(any(), any()));

        await().untilAsserted(() ->
                verify(serverIoEventsListener).onSSLSessionEnd(any(), any()));
    }

    /**
     * A peer that starts sending the instant its handshake is over, so that its last handshake record and its first
     * application record reach the server in the same read.
     * <p>
     * TLS 1.3 lets a client do exactly that - application data may follow its Finished immediately - and TCP is free
     * to deliver both in one segment. The server unwraps the Finished, declares the handshake done and switches to
     * its ordinary read path, which must carry over whatever else that read pulled in: those bytes are gone from the
     * socket, so dropping them costs the application a message and leaves the engine resuming mid record, which it
     * reports as a record longer than TLS allows.
     * <p>
     * The client engine is driven by hand rather than through an {@code SSLSocket} because the point of the test is
     * the framing: the final flight and the application record have to leave in a single write, which only writing
     * them to one buffer can guarantee.
     * <p>
     * The sizes matter. A payload below {@code IOSettings.readBufferSize} (8 KB) lands in the read buffer as it
     * stands; one above it does not, and the carry over has to grow that buffer first - the handshake reads into a
     * buffer of its own, sized from the engine, so it routinely holds more than the session's. Getting that growth
     * wrong throws {@link java.nio.BufferOverflowException} out of the IO worker, which is a dead session rather
     * than a slow one.
     */
    @ParameterizedTest(name = "{0} bytes riding with the Finished")
    @ValueSource(ints = {41, 24 * 1024})
    void testApplicationDataArrivingWithTheLastHandshakeRecordIsNotLost(int payloadSize) throws Exception {
        setupTestEnv(getTestClientBuilder(),
                getTestServerBuilder().toBuilder().serverSSLSettings(ServerSSLSettings.builder()
                        .sslContext(getServerSSLContext()).build()).build());
        server.start();

        byte[] payload = new byte[payloadSize];
        Arrays.fill(payload, (byte) 'x');
        AtomicInteger receivedBytes = new AtomicInteger();
        doAnswer(invocation -> {
            ByteBuffer read = invocation.getArgument(1);
            receivedBytes.addAndGet(read.remaining());
            read.position(read.limit());
            return null;
        }).when(serverIoEventsListener).onRead(any(IOSession.class), any(ByteBuffer.class), anyLong());

        try (SocketChannel socket = SocketChannel.open(new InetSocketAddress("localhost", port))) {
            SSLEngine engine = getClientSSLContext().createSSLEngine("localhost", port);
            engine.setUseClientMode(true);
            engine.beginHandshake();
            ByteBuffer outbound = ByteBuffer.allocate(64 * 1024);
            // held flipped, i.e. readable and empty: what one read pulls in past the record the engine is unwrapping
            // has to survive until the engine asks for it, exactly as it does on the session side of this test
            ByteBuffer inbound = ByteBuffer.allocate(engine.getSession().getPacketBufferSize()).flip();
            ByteBuffer decoded = ByteBuffer.allocate(engine.getSession().getApplicationBufferSize());

            // everything up to, but not including, the flight that completes the handshake. FINISHED is reported on
            // the result of the wrap that produces that flight and never by the engine afterwards, which is already
            // NOT_HANDSHAKING by then, so the result is what the buffer is held back on
            boolean finalFlightHeld = false;
            while (!finalFlightHeld) {
                switch (engine.getHandshakeStatus()) {
                    case NEED_TASK:
                        Runnable task;
                        while ((task = engine.getDelegatedTask()) != null) {
                            task.run();
                        }
                        break;
                    case NEED_WRAP:
                        outbound.clear();
                        finalFlightHeld = engine.wrap(ByteBuffer.allocate(0), outbound)
                                .getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED;
                        if (!finalFlightHeld) {
                            socket.write(outbound.flip());
                        }
                        break;
                    case NEED_UNWRAP:
                        // one record per turn of the outer loop, reading only when the buffer cannot feed the engine.
                        // Anything still in it stays there: the engine leaves the rest of a flight behind whenever it
                        // needs a task run or a wrap sent, and those bytes are gone from the socket already
                        if (!inbound.hasRemaining()) {
                            fill(socket, inbound);
                        }
                        decoded.clear();
                        if (engine.unwrap(inbound, decoded).getStatus() == SSLEngineResult.Status.BUFFER_UNDERFLOW) {
                            fill(socket, inbound);
                        }
                        break;
                    default:
                        throw new IllegalStateException("Unexpected handshake status " + engine.getHandshakeStatus());
                }
            }

            // the application records join the flight still sitting in the buffer, and all of it leaves in one write
            ByteBuffer application = ByteBuffer.wrap(payload);
            while (application.hasRemaining()) {
                assertThat(engine.wrap(application, outbound).getStatus()).isEqualTo(SSLEngineResult.Status.OK);
            }
            socket.write(outbound.flip());

            await().untilAtomic(receivedBytes, Matchers.equalTo(payload.length));
            verify(serverIoEventsListener, never()).onError(any(IOSession.class), any());
        }
    }

    /**
     * Appends to a buffer held flipped, keeping whatever the engine has not consumed yet. The read is capped well
     * below a record so that a handshake flight always arrives split across several of them: loopback usually hands
     * the whole flight over in one read, which hides a driver that drops what it has already taken off the socket -
     * on a busier machine it does not, and the engine then resumes in the middle of a record and reports a MAC
     * failure on the next one.
     */
    private static void fill(SocketChannel socket, ByteBuffer inbound) throws IOException {
        inbound.compact();
        int limit = inbound.limit();
        inbound.limit(Math.min(inbound.position() + 120, limit));
        assertThat(socket.read(inbound)).isPositive();
        inbound.limit(limit);
        inbound.flip();
    }

    private Certificate[] assetRemoteCertificate(String expected) {
        return assertArg(certs -> {
            assertThat(certs).hasSize(1);
            assertThat(certs[0]).isInstanceOf(X509Certificate.class);
            X509Certificate serverCert = (X509Certificate) certs[0];
            assertThat(serverCert.getSubjectDN().getName()).isEqualTo(expected);
        });
    }
}