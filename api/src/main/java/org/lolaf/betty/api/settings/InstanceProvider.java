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

import java.util.ServiceLoader;

/**
 * Implemented by a settings class that can build the thing it configures, so an application calls
 * {@code newInstance()} on the settings rather than naming an implementation class.
 *
 * @param <T> what the settings build
 */
public interface InstanceProvider<T> {

    /**
     * Finds the single registered {@link Factory} for an SPI and builds through it.
     *
     * @param settings the settings to build from
     * @param spiClass the SPI to look up
     * @param <I>      what is built
     * @param <F>      the factory type
     * @param <S>      the settings type
     * @return the new instance
     * @throws java.util.NoSuchElementException if no implementation is on the classpath - the usual cause being
     *                                          a dependency on the api module alone
     */
    static <I, F extends Factory<I, S>, S> I getSpiInstance(S settings, Class<F> spiClass) {
        return ServiceLoader.load(spiClass)
                .findFirst().orElseThrow()
                .newInstance(settings);
    }

    /**
     * Builds what these settings describe.
     *
     * @return a new instance; the settings stay reusable, so a second call builds a second one
     */
    T newInstance();
}
