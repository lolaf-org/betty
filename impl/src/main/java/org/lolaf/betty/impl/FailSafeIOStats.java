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

import org.lolaf.betty.api.io.IOSession;
import org.lolaf.betty.api.stats.IOStats;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;

/**
 * Wraps an application's {@link IOStats} so that a callback throwing logs instead of reaching the IO thread's
 * read or write loop - measurement must not be able to break the session it measures.
 * <p>
 * {@link #getTimeInNanos(IOStats.Operation)} and {@link #enabledByDefault()} are deliberately not wrapped: the
 * first is called twice per timed operation on the hot path and has to return a value rather than swallow a
 * failure, and the second is asked once, before any session is running.
 */
@Slf4j
@AllArgsConstructor
public class FailSafeIOStats implements IOStats {

    private final IOStats ioStats;

    @Override
    public boolean enabledByDefault() {
        return ioStats.enabledByDefault();
    }

    @Override
    public long getTimeInNanos(Operation operation) {
        return ioStats.getTimeInNanos(operation);
    }

    @Override
    public void onIOEventsListenerOnReadCall(IOSession ioSession, long methodCallStartTimeInNanos) {
        try {
            ioStats.onIOEventsListenerOnReadCall(ioSession, methodCallStartTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onIOEventsListenerOnReadCall()", ex);
        }
    }

    @Override
    public void onIOEventsListenerOnWriteCallback(IOSession ioSession, Object messageSendingContext, long methodCallStartTimeInNanos) {
        try {
            ioStats.onIOEventsListenerOnWriteCallback(ioSession, messageSendingContext, methodCallStartTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onIOEventsListenerOnWriteCallback()", ex);
        }
    }

    @Override
    public void onSocketRead(IOSession ioSession, int readenBytes, long localReadStartTimeInNanos) {
        try {
            ioStats.onSocketRead(ioSession, readenBytes, localReadStartTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onSocketRead()", ex);
        }
    }

    @Override
    public void onSocketWrite(IOSession ioSession, int writtenBytes, long localWriteStartTimeInNanos) {
        try {
            ioStats.onSocketWrite(ioSession, writtenBytes, localWriteStartTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onSocketWrite()", ex);
        }
    }

    @Override
    public void onMessageSent(IOSession ioSession, ByteBuffer sentMessage, Object messageSendingContext, long localSendingStartTimeInNanos) {
        try {
            ioStats.onMessageSent(ioSession, sentMessage, messageSendingContext, localSendingStartTimeInNanos);
        } catch (Exception ex) {
            log.error("Failed to call onMessageSent()", ex);
        }
    }

    @Override
    public void onSessionClosed(IOSession ioSession) {
        try {
            ioStats.onSessionClosed(ioSession);
        } catch (Exception ex) {
            log.error("Failed to call onSessionClosed()", ex);
        }
    }

    @Override
    public void onSessionOpened(IOSession ioSession) {
        try {
            ioStats.onSessionOpened(ioSession);
        } catch (Exception ex) {
            log.error("Failed to call onSessionOpened()", ex);
        }
    }
}
