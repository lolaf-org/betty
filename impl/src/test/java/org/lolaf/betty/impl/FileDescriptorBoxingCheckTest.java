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

import org.junit.jupiter.api.Test;

import java.util.function.LongPredicate;

import static org.assertj.core.api.Assertions.assertThat;

class FileDescriptorBoxingCheckTest {

    private static final String EPOLL = "sun.nio.ch.EPollSelectorImpl";
    private static final LongPredicate DEFAULT_CACHE = value -> value <= 127;

    @Test
    void warnsWhenTheDescriptorsOutgrowTheCache() {
        assertThat(FileDescriptorBoxingCheck.warning(EPOLL, 65536, DEFAULT_CACHE))
                .hasValueSatisfying(warning -> assertThat(warning).contains("-XX:AutoBoxCacheMax", "65536"));
    }

    @Test
    void warnsOnKqueue() {
        assertThat(FileDescriptorBoxingCheck.warning("sun.nio.ch.KQueueSelectorImpl", 65536, DEFAULT_CACHE)).isPresent();
    }

    @Test
    void isSilentOnceTheCacheIsWidened() {
        assertThat(FileDescriptorBoxingCheck.warning(EPOLL, 524288, value -> value <= 65536)).isEmpty();
    }

    @Test
    void isSilentWhenNoDescriptorCanPassTheDefaultCache() {
        assertThat(FileDescriptorBoxingCheck.warning(EPOLL, 129, DEFAULT_CACHE)).isPresent();
        assertThat(FileDescriptorBoxingCheck.warning(EPOLL, 128, DEFAULT_CACHE)).isEmpty();
    }

    @Test
    void isSilentForASelectorThatDoesNotBox() {
        assertThat(FileDescriptorBoxingCheck.warning("sun.nio.ch.PollSelectorImpl", 65536, DEFAULT_CACHE)).isEmpty();
    }

    @Test
    void isSilentWhenTheLimitIsUnknown() {
        assertThat(FileDescriptorBoxingCheck.warning(EPOLL, -1, DEFAULT_CACHE)).isEmpty();
    }
}
