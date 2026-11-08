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

import lombok.Getter;

import java.io.IOException;

/**
 * A message did not fit in the read buffer grown to {@code IOSettings.maxReadBufferSize}. Handed to
 * {@link IOEventsListener#onError}, after which the session is disconnected.
 */
@Getter
public class ReadBufferFullException extends IOException {

    private final int maxReadBufferSize;

    public ReadBufferFullException(String sessionId, int maxReadBufferSize) {
        super("Session " + sessionId + ": an incomplete message fills the read buffer at its maximum of "
                + maxReadBufferSize + " bytes, raise IOSettings.maxReadBufferSize");
        this.maxReadBufferSize = maxReadBufferSize;
    }
}
