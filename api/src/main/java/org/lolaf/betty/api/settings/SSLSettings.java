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
package org.lolaf.betty.api.settings;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.time.Duration;
import java.util.function.Consumer;

/**
 * TLS for a session. Supplying it is what makes a connection secure; leaving it null gives a plain socket.
 */
@Getter
@SuperBuilder(toBuilder = true)
public class SSLSettings {

    /**
     * SSL context to use for the secure connection
     */
    private final SSLContext sslContext;
    /**
     * SSLEngine created instance for the session consumer
     */
    @Builder.Default
    private Consumer<SSLEngine> engineSetupForSecureSession = s -> {
    };
    /**
     * How long a peer has to complete the TLS handshake before its session is closed with
     * {@code onFailedSSLHandshake}.
     * <p>
     * A peer that connects and then says nothing produces no readiness events at all, so nothing else would ever
     * notice it: without this deadline such a session would sit open indefinitely, holding its socket and its
     * buffers. Raise it for peers on slow links; {@link java.time.Duration#ZERO} disables the check entirely.
     */
    @Builder.Default
    private final Duration handshakeTimeout = Duration.ofSeconds(10);

}