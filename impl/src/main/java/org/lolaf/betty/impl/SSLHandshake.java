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

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLHandshakeException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * A TLS handshake driven by readiness events rather than by blocking, one instance per secure session, holding the
 * engine state and its buffers across the calls it takes to complete.
 * <p>
 * The channel is non-blocking - {@code IOWorkerImpl.registerInternal} configures it that way before the session
 * exists - so {@link SocketChannel#read} answers 0 whenever the peer has sent nothing yet, and
 * {@link SocketChannel#write} may take only part of what it is offered. Neither is an error and neither is a reason to
 * try again immediately: {@link #advance()} hands back what the handshake is waiting for and the caller returns to the
 * selector until the socket says it is ready. That is the whole point of this class. Spinning on a zero-length read
 * instead pinned an IO worker at 100% CPU for as long as a peer stayed silent, taking every other session on that
 * worker with it and leaving the engine unable to stop.
 */
@Slf4j
class SSLHandshake {

    private static final ByteBuffer EMPTY_ENCODING_WRITE_BUFFER = ByteBuffer.allocate(0);

    /**
     * What the handshake is waiting for, and therefore which readiness the caller should arm.
     */
    enum Progress {
        NEED_READ,
        NEED_WRITE,
        DONE
    }

    private final SocketChannel socketChannel;
    private final SSLEngine engine;
    private ByteBuffer decodedReadingBuffer;
    private ByteBuffer encodedReadingBuffer;
    private ByteBuffer encodedWriteBuffer;
    /**
     * Set when {@link #encodedWriteBuffer} still holds handshake bytes the socket would not take, which must go out
     * before anything else is wrapped.
     */
    private boolean flushPending;

    SSLHandshake(SocketChannel socketChannel, SSLEngine engine) {
        this.socketChannel = socketChannel;
        this.engine = engine;
        int appBufferSize = engine.getSession().getApplicationBufferSize();
        this.decodedReadingBuffer = ByteBuffer.allocate(appBufferSize);
        this.encodedReadingBuffer = ByteBuffer.allocate(appBufferSize);
        this.encodedWriteBuffer = ByteBuffer.allocate(appBufferSize);
    }

    void begin() throws IOException {
        engine.beginHandshake();
    }

    /**
     * Whatever the last read pulled in beyond the handshake itself, which the session has to take over once this is
     * {@link Progress#DONE}.
     * <p>
     * A read takes everything the socket has, and the peer is free to put application data right behind the record
     * that ends the handshake - TLS 1.3 lets a client send it immediately after its Finished, and TCP is free to
     * deliver both in one segment. Those bytes are gone from the socket, so nothing will ever hand them over again:
     * dropped here they cost the application a message, and leave the engine resuming in the middle of a record,
     * which it reports as a record longer than TLS allows rather than as the loss it is.
     *
     * @return the bytes in reading order, empty when the handshake consumed everything it read
     */
    ByteBuffer leftOverBytes() {
        return encodedReadingBuffer.duplicate().flip();
    }

    /**
     * Pumps the handshake as far as it can go without waiting, and answers what it needs next. Never blocks and never
     * repeats an operation that made no progress.
     */
    Progress advance() throws IOException {
        if (flushPending) {
            socketChannel.write(encodedWriteBuffer);
            if (encodedWriteBuffer.hasRemaining()) {
                return Progress.NEED_WRITE;
            }
            flushPending = false;
        }
        SSLEngineResult.HandshakeStatus handshakeStatus = engine.getHandshakeStatus();
        while (true) {
            switch (handshakeStatus) {
                case NEED_UNWRAP:
                    Progress unwrapProgress = unwrap();
                    if (unwrapProgress != null) {
                        return unwrapProgress;
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                case NEED_WRAP:
                    Progress wrapProgress = wrap();
                    if (wrapProgress != null) {
                        return wrapProgress;
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                case NEED_TASK:
                    Runnable runnable;
                    while ((runnable = engine.getDelegatedTask()) != null) {
                        runnable.run();
                    }
                    handshakeStatus = engine.getHandshakeStatus();
                    break;
                case FINISHED:
                    return Progress.DONE;
                case NOT_HANDSHAKING:
                    // the engine gave up on handshaking without ever reporting FINISHED
                    throw new SSLHandshakeException("Final handshake status is NOT_HANDSHAKING");
                default:
                    throw new IllegalStateException("Unhandled SSL status: " + handshakeStatus);
            }
        }
    }

    /**
     * One read-and-unwrap step. Answers null when it made progress and the caller should look at the engine again, or
     * the readiness to wait for when it could not.
     */
    private Progress unwrap() throws IOException {
        int readBytes = socketChannel.read(encodedReadingBuffer);
        if (readBytes < 0) {
            throw new SSLHandshakeException("Close socket");
        }
        if (readBytes == 0 && encodedReadingBuffer.position() == 0) {
            // nothing came and nothing is held over: waiting is the only thing to do, and the selector will say when
            return Progress.NEED_READ;
        }
        SSLEngineResult result = engine.unwrap(encodedReadingBuffer.flip(), decodedReadingBuffer);
        encodedReadingBuffer.compact();
        switch (result.getStatus()) {
            case OK:
                return result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED ? Progress.DONE : null;
            case BUFFER_OVERFLOW:
                decodedReadingBuffer = enlargeApplicationBuffer(engine, decodedReadingBuffer);
                return null;
            case BUFFER_UNDERFLOW:
                // a partial TLS record: the rest of it has to arrive before there is anything to do
                encodedReadingBuffer = handleBufferUnderflow(engine, encodedReadingBuffer);
                return Progress.NEED_READ;
            case CLOSED:
                throw new SSLHandshakeException("Close handshake");
            default:
                throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
        }
    }

    /**
     * One wrap-and-write step, with the same convention as {@link #unwrap()}.
     */
    private Progress wrap() throws IOException {
        SSLEngineResult result = engine.wrap(EMPTY_ENCODING_WRITE_BUFFER, encodedWriteBuffer.clear());
        switch (result.getStatus()) {
            case OK:
                boolean finished = result.getHandshakeStatus() == SSLEngineResult.HandshakeStatus.FINISHED;
                encodedWriteBuffer.flip();
                socketChannel.write(encodedWriteBuffer);
                if (encodedWriteBuffer.hasRemaining()) {
                    // the socket took only part of it, the rest goes out when it is writable again
                    flushPending = true;
                    return Progress.NEED_WRITE;
                }
                return finished ? Progress.DONE : null;
            case BUFFER_OVERFLOW:
                encodedWriteBuffer = enlargePacketBuffer(engine, encodedWriteBuffer);
                return null;
            case CLOSED:
                throw new SSLHandshakeException("Close handshake");
            default:
                throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
        }
    }

    private static ByteBuffer enlargeApplicationBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getApplicationBufferSize());
    }

    private static ByteBuffer enlargeBuffer(ByteBuffer buffer, int sessionProposedCapacity) {
        if (sessionProposedCapacity > buffer.capacity()) {
            return ByteBuffer.allocate(sessionProposedCapacity);
        }
        return ByteBuffer.allocate(buffer.capacity() * 2);
    }

    private static ByteBuffer enlargePacketBuffer(SSLEngine engine, ByteBuffer buffer) {
        return enlargeBuffer(buffer, engine.getSession().getPacketBufferSize());
    }

    private static ByteBuffer handleBufferUnderflow(SSLEngine engine, ByteBuffer buffer) {
        if (engine.getSession().getPacketBufferSize() < buffer.limit()) {
            return buffer;
        }
        return enlargePacketBuffer(engine, buffer).put(buffer.flip());
    }
}
