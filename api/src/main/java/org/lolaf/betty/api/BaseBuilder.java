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
import org.lolaf.betty.api.io.IOWorkersGroup;
import org.lolaf.betty.api.settings.IOSettings;
import org.lolaf.betty.api.stats.IOStats;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

/**
 * What a {@link ClientBuilder} and a {@link ServerBuilder} have in common: the listener, the IO settings and the
 * threads that will run them.
 * <p>
 * A builder is kept by the client or server it creates and stays readable through {@code getBuilder()}, so it is
 * the record of what an instance is running with.
 */
@Getter
@SuperBuilder(toBuilder = true)
public abstract class BaseBuilder {
    /**
     * Id of the client or server
     */
    @Builder.Default
    private final String id = "default";
    /**
     * IO events listener
     */
    private final IOEventsListener ioEventsListener;
    /**
     * IO settings for each socket established
     */
    @Builder.Default
    private final IOSettings ioSettings = IOSettings.builder().build();

    /**
     * IO worker group for processing connections reads/writes, if null a default one with one thread will be created
     */
    private final IOWorkersGroup ioWorkersGroup;

    /**
     * Called once per session to decide what statistics it collects. The default collects none, statistics being
     * work on the IO thread; {@code enableIOStats} then turns collection on and off at runtime.
     */
    @Builder.Default
    private final IOStats.IOStatsProvider ioStatsProvider = newIoSession -> IOStats.VoidStats.getInstance();

}
