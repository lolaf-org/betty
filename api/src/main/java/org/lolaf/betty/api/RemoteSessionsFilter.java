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
package org.lolaf.betty.api;

import org.lolaf.betty.api.io.IOEventsListener;

import java.net.InetSocketAddress;
import java.security.cert.Certificate;

/**
 * Decides whether an accepted connection is kept, consulted once per session before it is usable.
 * <p>
 * When it turns a session down the listener sees {@code onSessionRejected} and the socket is closed, so
 * {@code onConnected} never fires for it.
 */
public interface RemoteSessionsFilter {

    /**
     * Called on the session's IO thread, so it must not block.
     *
     * @param remoteAddress      the peer's address
     * @param remoteCertificates the peer's certificate chain, {@code null} on a plain session and on a TLS session
     *                           whose peer did not present one. On TLS this is called after the handshake, which is
     *                           what makes the chain available and why {@link IOEventsListener#onConnected} is
     *                           later there than on a plain session
     * @return true to keep the session, false to close it
     */
    boolean allow(InetSocketAddress remoteAddress, Certificate[] remoteCertificates);
}
