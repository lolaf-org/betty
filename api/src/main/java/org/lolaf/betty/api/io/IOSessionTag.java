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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A label put on sessions so that a broadcast can select them without holding references to them.
 * <p>
 * Interned by value, so tags compare by identity and {@code Server.broadcast} matches without comparing strings
 * on the sending path. The flip side is that a tag is never collected: they are meant to be few and fixed, not
 * derived per session.
 */
@Getter
public class IOSessionTag {

    private static final Map<String, IOSessionTag> VALUES = new ConcurrentHashMap<>();

    private final String value;

    private IOSessionTag(String value) {
        this.value = value;
    }

    /**
     * The tag for a value, created on first use and shared thereafter.
     *
     * @param value the tag's value
     * @return the one instance for that value
     */
    public static IOSessionTag of(String value) {
        return VALUES.computeIfAbsent(value, IOSessionTag::new);
    }
}
