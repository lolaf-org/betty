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

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.lolaf.betty.api.io.IOEventsListener;
import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.settings.IOSettings;

import javax.net.ssl.SSLHandshakeException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.cert.Certificate;

@AllArgsConstructor
@Slf4j
class FailSafeIOEventsListener implements IOEventsListener {

    private final IOEventsListener ioEventsListener;

    @Override
    public IOSettings getIOSettings(InetSocketAddress remoteAddress, IOSettings defaultSettings) {
        try {
            return ioEventsListener.getIOSettings(remoteAddress, defaultSettings);
        } catch (Exception ex) {
            log.error("Failed to call getIOSettings()", ex);
        }
        return defaultSettings;
    }

    @Override
    public boolean isReadyToConnect(InetSocketAddress remoteAddress) {
        try {
            return ioEventsListener.isReadyToConnect(remoteAddress);
        } catch (Exception ex) {
            log.error("Failed to call isReadyToConnect()", ex);
        }
        return true;
    }

    @Override
    public void onConnected(IOSession session) {
        try {
            ioEventsListener.onConnected(session);
        } catch (Exception ex) {
            log.error("Failed to call onConnection()", ex);
        }
    }

    @Override
    public void onDisconnected(IOSession session) {
        try {
            ioEventsListener.onDisconnected(session);
        } catch (Exception ex) {
            log.error("Failed to call onDisconnection()", ex);
        }
    }

    @Override
    public void onRead(IOSession session, ByteBuffer message, long localReceiveTimeInNanos) {
        try {
            ioEventsListener.onRead(session, message, localReceiveTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onRead()", ex);
            onError(session, ex);
        }
    }

    @Override
    public void onWrite(IOSession session, ByteBuffer message) {
        try {
            ioEventsListener.onWrite(session, message);
        } catch (Exception ex) {
            log.error("Failed to call onWrite()", ex);
            onError(session, ex);
        }
    }

    @Override
    public void onWriteFailure(IOSession session, ByteBuffer message) {
        try {
            ioEventsListener.onWriteFailure(session, message);
        } catch (Exception ex) {
            log.error("Failed to call onWriteFailure()", ex);
        }
    }

    @Override
    public void onError(IOSession session, Exception error) {
        try {
            ioEventsListener.onError(session, error);
        } catch (Exception ex) {
            log.error("Failed to call onError()", ex);
        }
    }

    @Override
    public void onShutdown(IOSession session) {
        try {
            ioEventsListener.onShutdown(session);
        } catch (Exception ex) {
            log.error("Failed to call onShutdown()", ex);
        }
    }

    @Override
    public void onFailedSSLHandshake(IOSession session, SSLHandshakeException exception) {
        try {
            ioEventsListener.onFailedSSLHandshake(session, exception);
        } catch (Exception ex) {
            log.error("Failed to call onFailedSSLHandshake()", ex);
        }
    }

    @Override
    public void onSSLHandshake(IOSession session, Certificate[] remotePeerCertificates) {
        try {
            ioEventsListener.onSSLHandshake(session, remotePeerCertificates);
        } catch (Exception ex) {
            log.error("Failed to call onSSLHandshake()", ex);
        }
    }

    @Override
    public void onSessionRejected(IOSession session) {
        try {
            ioEventsListener.onSessionRejected(session);
        } catch (Exception ex) {
            log.error("Failed to call onSessionRejected()", ex);
        }
    }

    @Override
    public void onWatermarkEvent(IOSession session, boolean highWatermarkReached, long bytesLeftToWrite) {
        try {
            ioEventsListener.onWatermarkEvent(session, highWatermarkReached, bytesLeftToWrite);
        } catch (Exception ex) {
            log.error("Failed to call onWatermarkEvent()", ex);
        }
    }

    @Override
    public void onSSLSessionEnd(IOSession session, Certificate[] remotePeerCertificates) {
        try {
            ioEventsListener.onSSLSessionEnd(session, remotePeerCertificates);
        } catch (Exception ex) {
            log.error("Failed to call onSSLSessionEnd()", ex);
        }
    }
}
