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

import com.sun.management.UnixOperatingSystemMXBean;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.nio.channels.Selector;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongPredicate;

/**
 * Warns once per JVM when the JDK selector allocates on the IO threads: its epoll and kqueue implementations look each
 * ready key up by its file descriptor boxed as an {@code Integer}, which allocates for a descriptor above the
 * {@code Integer} cache.
 */
@Slf4j
@UtilityClass
class FileDescriptorBoxingCheck {

    private static final Set<String> BOXING_SELECTORS = Set.of("sun.nio.ch.EPollSelectorImpl", "sun.nio.ch.KQueueSelectorImpl");
    private static final AtomicBoolean CHECKED = new AtomicBoolean();
    private static final int DEFAULT_CACHE_HIGH = 127;

    static void check(Selector selector) {
        if (!CHECKED.getAndSet(true)) {
            warning(selector.getClass().getName(), maxFileDescriptors(), FileDescriptorBoxingCheck::isCached).ifPresent(log::warn);
        }
    }

    /**
     * Only a cache left at its default is reported: one widened on purpose but short of the limit is the operator's
     * call, since the limit is often far above the descriptors a process really uses.
     *
     * @param maxFileDescriptors the process's descriptor limit, negative when unknown
     */
    static Optional<String> warning(String selectorClassName, long maxFileDescriptors, LongPredicate cached) {
        if (!BOXING_SELECTORS.contains(selectorClassName) || maxFileDescriptors <= DEFAULT_CACHE_HIGH + 1
                || cached.test(DEFAULT_CACHE_HIGH + 1)) {
            return Optional.empty();
        }
        return Optional.of(String.format("The JDK selector allocates an Integer on every ready event of a file descriptor"
                + " above 127, and this process may open up to %d: set -XX:AutoBoxCacheMax to the highest descriptor it"
                + " uses, at most %d, to keep the IO threads allocation free", maxFileDescriptors, maxFileDescriptors));
    }

    private static boolean isCached(long value) {
        return value <= Integer.MAX_VALUE && Integer.valueOf((int) value) == Integer.valueOf((int) value);
    }

    private static long maxFileDescriptors() {
        try {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            return os instanceof UnixOperatingSystemMXBean ? ((UnixOperatingSystemMXBean) os).getMaxFileDescriptorCount() : -1;
        } catch (LinkageError absentManagementModule) {
            return -1;
        }
    }
}
