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
import org.lolaf.betty.api.settings.ServerSSLSettings;
import org.lolaf.betty.api.ss.SelectStrategy;
import org.lolaf.betty.api.stats.IOStats;
import org.lolaf.ringos.Deadline;

import javax.net.ssl.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.security.cert.Certificate;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Slf4j
class SecureIOSession extends IOSessionImpl {

    /**
     * How long {@link #closeOutboundOnIOThread()} waits for the IO thread to send the close_notify. Short on
     * purpose: this is on the shutdown path, and dropping the record beats delaying every close behind a worker
     * that is not coming back.
     */
    private static final Duration CLOSE_NOTIFY_HANDOVER_TIMEOUT = Duration.ofMillis(500);
    private final SSLEngine sslEngine;
    private final ByteBuffer[] sourceByteBufferArray;
    private final ByteBuffer encodingWriteBuffer;
    /** True while {@link #encodingWriteBuffer} is flipped and holds a record the socket has not finished taking. */
    private boolean encodedBytesPending;
    private final ByteBuffer[] decodingReadBuffer;
    private final Duration handshakeTimeout;
    /**
     * The handshake in progress, and whether it still is. Both are touched only by the IO worker thread owning the
     * session, except {@link #handshaking} which the worker loop reads while sweeping for expired handshakes.
     */
    private SSLHandshake handshake;
    private volatile boolean handshaking = true;
    private volatile long handshakeDeadlineInNanos;

    public SecureIOSession(boolean clientSession, SocketChannel socket, BaseBuilder baseBuilder, IOSettings ioSettings, RemoteSessionsFilter remoteSessionsFilter,
                           IOBufferPool sharedIOBufferPool, IOWorkersGroupSettings ioWorkersGroupSettings) {
        super(clientSession, socket, baseBuilder, ioSettings, remoteSessionsFilter,
                sharedIOBufferPool, ioWorkersGroupSettings);
        if (baseBuilder instanceof ServerBuilder) {
            ServerSSLSettings sslSettings = ((ServerBuilder) baseBuilder).getServerSSLSettings();
            sslEngine = getSslEngine(sslSettings.getSslContext(), false);
            sslEngine.setNeedClientAuth(sslSettings.isNeedClientAuth());
            sslEngine.setWantClientAuth(sslSettings.isWantClientAuth());
            handshakeTimeout = sslSettings.getHandshakeTimeout();
            callListenerForEngineSetup(sslSettings);
        } else {
            SSLSettings sslSettings = ((ClientBuilder) baseBuilder).getSSLSettings();
            sslEngine = getSslEngine(sslSettings.getSslContext(), true);
            handshakeTimeout = sslSettings.getHandshakeTimeout();
            callListenerForEngineSetup(sslSettings);
        }
        sourceByteBufferArray = new ByteBuffer[1];
        encodingWriteBuffer = getIoBufferPool().newByteBuffer(sslEngine.getSession().getPacketBufferSize());
        // lib users may want to have direct access to underyling byte array when reading which is only possible when using
        // non direct byte array so respect isReadDirectBuffer to allocate correctly read buffer and do not use pool settings
        ByteBuffer readBuffer = ioSettings.isReadDirectBuffer()
                ? ByteBuffer.allocateDirect(sslEngine.getSession().getPacketBufferSize())
                : ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        decodingReadBuffer = new ByteBuffer[]{readBuffer};
    }

    private void callListenerForEngineSetup(SSLSettings sslSettings) {
        try {
            sslSettings.getEngineSetupForSecureSession().accept(sslEngine);
        } catch (Exception ex) {
            log.error("Failed to call engineSetupForSecureSession", ex);
        }
    }

    private SSLEngine getSslEngine(SSLContext sslSettings, boolean clientMode) {
        SSLEngine sslEngineInstance = sslSettings.createSSLEngine(getSocketAddress().getHostName(), getSocketAddress().getPort());
        sslEngineInstance.setUseClientMode(clientMode);
        return sslEngineInstance;
    }

    @Override
    boolean isConnectionAllowed() {
        return getRemoteSessionsFilter().allow(getSocketAddress(), getRemotePeerCertificates());
    }

    private Certificate[] getRemotePeerCertificates() {
        try {
            return sslEngine.getSession().getPeerCertificates();
        } catch (SSLPeerUnverifiedException e) {
            return null;
        }
    }

    /**
     * Puts the session on the selector and starts the handshake, rather than completing it here.
     * <p>
     * The order is the reverse of {@link IOSessionImpl#start}, and has to be: this session cannot answer
     * {@link #isConnectionAllowed()} until it holds the peer's certificates, which the handshake has yet to produce,
     * and the handshake cannot make progress until the selection key exists to tell it when the socket is ready.
     * Registering first and deciding afterwards is what lets the handshake be driven rather than waited on.
     * <p>
     * Answers true so that the session is registered with its worker while still handshaking, which is what makes it
     * reachable by {@code stop} - and by the handshake deadline.
     */
    @Override
    boolean start(Selector selector, SelectStrategy selectStrategy, Consumer<IOSession> sessionStoppedConsumer, Consumer<IOSession> tasksRingBufferFullConsumer,
                  Consumer<IOSession> writesRingBufferFullConsumer, Thread ioWorkerThread, IOWorker ioWorker) throws IOException {
        log.debug("SSL handshake initiated for {}", getId());
        // before anything can fail: stop() does nothing to a session that was never marked started, and from here on
        // there is a socket registered with a selector that has to be closed however the handshake turns out
        getStarted().set(true);
        prepareForIO(selector, selectStrategy, sessionStoppedConsumer, tasksRingBufferFullConsumer,
                writesRingBufferFullConsumer, ioWorkerThread, ioWorker);
        handshake = new SSLHandshake(getSocket(), sslEngine);
        handshakeDeadlineInNanos = handshakeTimeout.isZero() ? 0L : System.nanoTime() + handshakeTimeout.toNanos();
        handshake.begin();
        advanceHandshake(System.nanoTime());
        return true;
    }

    /**
     * Moves the handshake on by whatever the socket has made possible, and arms the readiness it is waiting for next.
     * A failure ends the session here rather than propagating: by this point the session is registered, so it has to
     * be taken down the same way any other broken one is.
     *
     * @param localReceiveTimeInNanos when the readiness that led here was observed, carried through for the
     *                                application data a completing handshake may hand over with its last record
     */
    private void advanceHandshake(long localReceiveTimeInNanos) {
        try {
            switch (handshake.advance()) {
                case NEED_READ:
                    setInterestOps(SelectionKey.OP_READ);
                    break;
                case NEED_WRITE:
                    setInterestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
                    break;
                case DONE:
                    onHandshakeDone(localReceiveTimeInNanos);
                    break;
                default:
                    throw new IllegalStateException("Unhandled handshake progress");
            }
        } catch (IOException e) {
            failHandshake(e);
        }
    }

    /**
     * The handshake is over, so the session becomes an ordinary one: the certificate filter can finally be consulted,
     * and only now does {@code onConnected} fire - it means "TLS is up" to everything listening for it, and moving it
     * earlier would hand callers a session they cannot yet write to.
     */
    private void onHandshakeDone(long localReceiveTimeInNanos) throws IOException {
        handshakeDeadlineInNanos = 0L;
        handshaking = false;
        if (!isConnectionAllowed()) {
            log.info("SSL session {} rejected by the remote sessions filter", getId());
            getIoEventsListener().onSessionRejected(this);
            stop(Deadline.immediate());
            return;
        }
        getIoEventsListener().onSSLHandshake(this, getRemotePeerCertificates());
        log.info("SSL handshake success for {}", getId());
        setInterestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        notifyConnected();
        // last, so that a peer which sent application data behind its last handshake record still sees onConnected
        // before the read it carried
        processHandshakeLeftOver(localReceiveTimeInNanos);
    }

    /**
     * Takes over the bytes the handshake read past its own last record and feeds them through the ordinary read path.
     * <p>
     * They have already been taken off the socket, so no readiness event will ever bring them back: waiting for the
     * next one loses them, and loses them silently - the peer believes the message was delivered, while this side
     * carries on decrypting from the middle of a record and reports whatever the next two bytes happen to say as a
     * record length.
     *
     * @see SSLHandshake#leftOverBytes()
     */
    private void processHandshakeLeftOver(long localReceiveTimeInNanos) throws IOException {
        ByteBuffer leftOver = handshake.leftOverBytes();
        if (!leftOver.hasRemaining()) {
            return;
        }
        log.debug("Carrying over {} bytes read with the last handshake record of {}", leftOver.remaining(), getId());
        // the handshake reads into a buffer of its own, sized from the engine's application buffer, so it can hand
        // over more than a read buffer sized from IOSettings holds. Growing to fit is not optional: these bytes are
        // off the socket already, and there is nowhere to put them back
        ByteBuffer readBuffer = getReadBuffer();
        setReadBuffer(readBuffer = growReadBufferFor(readBuffer, leftOver.remaining()));
        int leftOverBytes = leftOver.remaining();
        readBuffer.put(leftOver);
        readBuffer.flip();
        IOStats ioStats = getActiveIOStats();
        unwrapReadBuffer(readBuffer, leftOverBytes, localReceiveTimeInNanos, ioStats,
                ioStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_READ));
    }

    /**
     * Ends a session whose handshake could not be completed.
     * <p>
     * Anything the TLS engine raises while handshaking is reported as a failed handshake, whichever
     * {@link SSLException} subclass the JDK chose for it - a plaintext client pointed at a TLS port raises a plain
     * {@code SSLException} ("Unrecognized SSL message, plaintext connection?"), which is a handshake failure by any
     * useful reading and is worth naming as one. Anything else, a reset socket and the like, stays an error.
     */
    private void failHandshake(IOException e) {
        handshakeDeadlineInNanos = 0L;
        handshaking = false;
        if (e instanceof SSLHandshakeException) {
            getIoEventsListener().onFailedSSLHandshake(this, (SSLHandshakeException) e);
        } else if (e instanceof SSLException) {
            SSLHandshakeException failure = new SSLHandshakeException(e.getMessage());
            failure.initCause(e);
            getIoEventsListener().onFailedSSLHandshake(this, failure);
        } else {
            getIoEventsListener().onError(this, e);
        }
        stop(Deadline.immediate());
    }

    /**
     * Ends a handshake that has run out of time, which is the only way a peer that connects and then says nothing is
     * ever noticed: it sends nothing, so it produces no readiness events, so nothing else here would ever look at it.
     * Called from the IO worker loop, and only while handshakes are outstanding.
     */
    void expireHandshakeIfTimedOut(long nowInNanos) {
        if (!handshaking || handshakeDeadlineInNanos == 0L || nowInNanos - handshakeDeadlineInNanos < 0) {
            return;
        }
        log.warn("SSL handshake for {} did not complete within {}, closing", getId(), handshakeTimeout);
        failHandshake(new SSLHandshakeException("SSL handshake did not complete within " + handshakeTimeout));
    }

    boolean isHandshaking() {
        return handshaking;
    }

    /**
     * While handshaking the socket becoming writable means the handshake has bytes to put out, not that the
     * application does: nothing may be wrapped as application data until the engine says the handshake is over.
     */
    @Override
    void onOperationWriteInternal() throws IOException {
        if (handshaking) {
            // a write readiness rather than a read, so nothing was received now: the handshake may still read from
            // the socket on its way through, and anything it carries over is timestamped as of here
            advanceHandshake(System.nanoTime());
            return;
        }
        super.onOperationWriteInternal();
    }

    @Override
    protected void disconnect() {
        closeOutboundOnIOThread();
        getIoEventsListener().onSSLSessionEnd(this, getRemotePeerCertificates());
        super.disconnect();
    }

    /**
     * Sends the close_notify from the thread that owns every other engine call and every other socket write, rather
     * than from whichever thread called {@code stop()}.
     * <p>
     * Both halves of {@link #sendCloseOutboundIfNeeded()} are unsafe off the IO thread. An {@link SSLEngine} is not
     * thread safe across {@code wrap} and {@code unwrap} - they share the record sequence numbers a GCM nonce is
     * built from - and a direct {@code socket.write} from a second thread can interleave the close_notify record
     * into the middle of an application record the IO thread is still writing. The peer then reads either a length
     * field out of alignment or a record whose MAC fails, both on its inbound path.
     * <p>
     * Waits, because {@link IOSessionImpl#disconnect()} cancels the key and closes the socket immediately after: a
     * close_notify still queued at that point would be written to a closed socket. If the handover does not complete
     * - an IO worker already on its way out has nobody left to run the task - the close_notify is dropped rather
     * than sent from here. An unclean shutdown costs the peer a truncation warning; a raced one corrupts its stream,
     * which is the thing being fixed.
     */
    private void closeOutboundOnIOThread() {
        if (isIOThread(Thread.currentThread())) {
            sendCloseOutboundIfNeeded();
            return;
        }
        CountDownLatch sent = new CountDownLatch(1);
        try {
            processTask(() -> {
                try {
                    sendCloseOutboundIfNeeded();
                } finally {
                    sent.countDown();
                }
            });
            if (!sent.await(CLOSE_NOTIFY_HANDOVER_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                log.info("Timed out handing close_notify to the IO thread of session {}, closing without it", getId());
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log.info("Unable to hand close_notify to the IO thread of session {}, closing without it: {}", getId(), ex.getMessage());
        }
    }

    @Override
    protected int writeToSocket(ByteBuffer bufferOut) throws IOException {
        // always flip and mark buffer or when isOutboundDone message will be processed badly by exception handlers
        bufferOut.flip().mark();
        return continueWriteToSocket(bufferOut);
    }

    @Override
    protected int continueWriteToSocket(ByteBuffer bufferOut) throws IOException {
        if (sslEngine.isOutboundDone()) {
            throw new SSLException("SSL engine outbound is done");
        }
        // a record the socket refused last time goes out before anything else is wrapped, or its bytes would be
        // overwritten and the peer would see a corrupt record
        int bytesWritten = flushEncodedBytes();
        if (encodedBytesPending) {
            return bytesWritten;
        }
        sourceByteBufferArray[0] = bufferOut; // no concurrent access on this method call so no problem
        while (bufferOut.hasRemaining()) {
            SSLEngineResult result = sslEngine.wrap(sourceByteBufferArray, encodingWriteBuffer);
            switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    throw new IllegalStateException("BUFFER_OVERFLOW while writing, should have never happened: " + encodingWriteBuffer.capacity());
                case CLOSED:
                    throw new IOException("Should have never happened");
                case OK:
                    bytesWritten += flushEncodedBytes();
                    if (encodedBytesPending) {
                        return bytesWritten;
                    }
                    break;
                default:
                    log.error("Not handled case {}", result);
                    return bytesWritten;
            }
        }
        return bytesWritten;
    }

    private int flushEncodedBytes() throws IOException {
        if (!encodedBytesPending) {
            if (encodingWriteBuffer.position() == 0) {
                return 0;
            }
            encodingWriteBuffer.flip();
            encodedBytesPending = true;
        }
        int bytesWritten = super.continueWriteToSocket(encodingWriteBuffer);
        if (!encodingWriteBuffer.hasRemaining()) {
            encodingWriteBuffer.clear();
            encodedBytesPending = false;
        }
        return bytesWritten;
    }

    @Override
    void onOperationReadInternal(long localReceiveTimeInNanos) throws IOException {
        if (handshaking) {
            advanceHandshake(localReceiveTimeInNanos);
            return;
        }
        IOStats ioStats = getActiveIOStats();
        long localReadStartTime = ioStats.getTimeInNanos(IOStats.Operation.IO_SOCKET_READ);
        ByteBuffer readBuffer = getReadBuffer();
        int readenBytes = super.readFromSocket(readBuffer);
        readBuffer.flip();
        unwrapReadBuffer(readBuffer, readenBytes, localReceiveTimeInNanos, ioStats, localReadStartTime);
    }

    /**
     * Unwraps every record the read buffer holds, handing each record's application data up as it comes.
     *
     * @param readBuffer the encrypted bytes, flipped and ready to be read from
     */
    private void unwrapReadBuffer(ByteBuffer readBuffer, int readenBytes, long localReceiveTimeInNanos,
                                  IOStats ioStats, long localReadStartTime) throws IOException {
        ByteBuffer decodedBuffer = decodingReadBuffer[0];
        while (true) {
            SSLEngineResult result = sslEngine.unwrap(readBuffer, decodingReadBuffer);
            switch (result.getStatus()) {
                case BUFFER_OVERFLOW:
                    throw new IllegalStateException("BUFFER_OVERFLOW while reading, should have never happened: " + encodingWriteBuffer.capacity());
                case BUFFER_UNDERFLOW:
                    // our socket buffer is too small compared to ssl max packet size, increase its size and retry
                    // decoding.
                    if (sslEngine.getSession().getPacketBufferSize() > readBuffer.capacity()) {
                        setReadBuffer(resizeReadBuffer(readBuffer, 0.3d, sslEngine.getSession().getPacketBufferSize()));
                        ioStats.onSocketRead(this, readenBytes, localReadStartTime);
                        return;
                    }
                    // we are really missing some more data from socket
                    readBuffer.compact();
                    ioStats.onSocketRead(this, readenBytes, localReadStartTime);
                    return;
                case CLOSED:
                    sendCloseOutboundIfNeeded();
                    sslEngine.closeInbound();
                    return;
                case OK:
                    if (decodedBuffer.position() > 0) {
                        // a record can be consumed without producing any application data - the TLS 1.3 post
                        // handshake messages, NewSessionTicket and KeyUpdate, are records like any other - and there
                        // is nothing to hand up when that happens
                        processIOEventsListenerOnRead(decodedBuffer.flip(), localReceiveTimeInNanos);
                    }
                    // we managed to fully read the input buffer, clear it and return
                    if (!readBuffer.hasRemaining()) {
                        readBuffer.clear();
                        ioStats.onSocketRead(this, readenBytes, localReadStartTime);
                        return;
                    }
                    break;
                default:
                    log.error("Not handled case {}", result);
                    return;
            }
        }
    }

    private void sendCloseOutboundIfNeeded() {
        if (!isIOThread(Thread.currentThread())) {
            log.error("SSLENGINE-OFF-IO-THREAD operation={} thread={} handshaking={}",
                    "sendCloseOutboundIfNeeded", Thread.currentThread().getName(), handshaking, new IllegalStateException("call site"));
        }
        if (sslEngine.isOutboundDone()) {
            return;
        }
        ByteBuffer out = ByteBuffer.allocate(sslEngine.getSession().getPacketBufferSize());
        sslEngine.closeOutbound();
        try {
            sslEngine.wrap(ByteBuffer.allocate(0), out);
            getSocket().write(out.flip());
        } catch (IOException e) {
            log.warn("Failed to send closeOutbound {}", e.getMessage());
        }
    }
}