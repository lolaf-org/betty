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
package org.lolaf.betty.api.io;

import org.lolaf.betty.api.RemoteSessionsFilter;
import org.lolaf.betty.api.settings.IOSettings;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLHandshakeException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;
import java.util.function.BiConsumer;

/**
 * Every callback an application gets from a session, and the only place connection state is reported.
 * <p>
 * All of them run on the session's IO thread, which is shared with every other session on the same
 * {@link IOWorker}: whatever they do is time no other session gets. A callback that throws does not take the
 * session down - the implementation logs it and carries on - so a failure here is not a way to refuse a message.
 */
public interface IOEventsListener {

    /**
     * Allows to tweak default io setting to apply for a given remote address connection, called before {@link #onConnected(IOSession)}
     *
     * @param remoteAddress   the remote address that is connecting
     * @param defaultSettings the default configure IOSettings
     * @return the IOSettings ot apply to the new IO session
     */
    default IOSettings getIOSettings(InetSocketAddress remoteAddress, IOSettings defaultSettings) {
        return defaultSettings;
    }

    /**
     * Asked before every connection attempt a {@link org.lolaf.betty.api.Client} makes, and only by a client - a
     * server never consults it. Answering false skips that attempt without opening a socket, and the client asks
     * again after {@link org.lolaf.betty.api.ClientBuilder#getConnectionRetry()}, so a listener that is not ready
     * yet has nothing to arrange: it keeps answering false for as long as that lasts. A listener throwing from here
     * is taken as ready, which is what leaves a connection retried rather than abandoned.
     * <p>
     * This is what keeps a client that knows it has nothing to do - outside the hours it operates, or shut down by
     * an operator - from dialing a peer every retry interval only to be dropped, for as long as that lasts.
     *
     * @param remoteAddress the address this attempt would connect to
     * @return true to let the attempt proceed
     */
    default boolean isReadyToConnect(InetSocketAddress remoteAddress) {
        return true;
    }

    /**
     * Callback when a session is connected. On a TLS session this is after the handshake, so it means the session
     * is writable rather than merely accepted.
     *
     * @param session the session, usable from here on
     */
    default void onConnected(IOSession session) {

    }

    /**
     * Indicates a disconnection from a remote IOSession
     *
     * @param session the new io session or null if a disconnection occurred before being able to set up the session
     */
    default void onDisconnected(IOSession session) {

    }

    /**
     * Indicates the session is being shut down, called before onDisconnected
     *
     * @param session the session shutting down
     */
    default void onShutdown(IOSession session) {

    }

    /**
     * Indicates data can be read from the socket, buffer will be compacted automatically after each call to this method
     *
     * @param session                 the io session
     * @param message                 the message to read
     * @param localReceiveTimeInNanos the local received time in nanos, controlled by {@link IOSettings#isTrackReceiveTime()}
     */
    void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos);

    /**
     * Called when a message write has occurred using an API call without callbacks like {@link IOWriter#send(ByteBuffer, boolean)} or {@link IOWriter#send(byte[])}
     *
     * @param session the session that wrote it
     * @param message the message as it went out; the buffer is recycled once this returns
     */
    default void onWrite(IOSession session, ByteBuffer message) {

    }

    /**
     * Called when a message write failure has occurred using an API call without callbacks like {@link IOWriter#send(ByteBuffer, boolean)} or {@link IOWriter#send(byte[])}
     *
     * @param session the session that failed to write
     * @param message the message that did not go out
     */
    default void onWriteFailure(IOSession session, ByteBuffer message) {

    }

    /**
     * Called when a task need to be executed within the IO thread, can be overridden to send the task to execute to another thread if needed
     *
     * @param session      the IOSession from which the task has been triggered
     * @param task         the task to execute
     * @param taskCallback the callback to call when the task is executed
     */
    default void onTask(IOSession session, Runnable task, BiConsumer<Runnable, Exception> taskCallback) {
        task.run();
        if (taskCallback != null) {
            taskCallback.accept(task, null);
        }
    }

    /**
     * Called when an error occurred on the session. The default logs it, so an override that does not log loses it.
     *
     * @param session the session the error occurred on
     * @param error   what went wrong
     */
    default void onError(IOSession session, Exception error) {
        LoggerFactory.getLogger(getClass()).error("Error occurred on IOSession {}", session, error);
    }

    /**
     * Failed SSL handshake notification
     *
     * @param session   the io session
     * @param exception the handshake exception
     */
    default void onFailedSSLHandshake(IOSession session, SSLHandshakeException exception) {
        LoggerFactory.getLogger(getClass()).info("Failed ssl handshake: {}", exception.getMessage());
    }

    /**
     * SSL handshake notification, called before {@link #onConnected(IOSession)}
     *
     * @param session                the io session
     * @param remotePeerCertificates the remote certificates or null if none provided
     */
    default void onSSLHandshake(IOSession session, Certificate[] remotePeerCertificates) {

    }

    /**
     * SSL session end notification, called before {@link #onDisconnected(IOSession)}
     *
     * @param session                the io session
     * @param remotePeerCertificates the remote certificates or null if none provided
     */
    default void onSSLSessionEnd(IOSession session, Certificate[] remotePeerCertificates) {

    }

    /**
     * Indicates that an incoming session has been rejected by {@link RemoteSessionsFilter#allow(InetSocketAddress, Certificate[])}
     *
     * @param session the rejected session, will be disconnected immediately after this method call
     */
    default void onSessionRejected(IOSession session) {

    }

    /**
     * Callback for network watermark events when thresholds defined by {@link IOSettings#getWriteHighWatermark()} and {@link IOSettings#getWriteLowWatermark()} are reached
     * This typically indicates that remote io session has troubles consuming messages at the current sending rate
     *
     * @param session              the io session that reaches the watermark event
     * @param highWatermarkReached true when bytes left to write are above thresholds defined
     *                             by {@link IOSettings#getWriteHighWatermark()} or false when under {@link IOSettings#getWriteLowWatermark()} thresholds
     * @param bytesLeftToWrite     the number of bytes left to write to a remote socket when the event is triggered
     */
    default void onWatermarkEvent(IOSession session, boolean highWatermarkReached, long bytesLeftToWrite) {

    }
}