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

/**
 * The shape of every SPI in betty: the implementation module registers one of these, and the api module reaches an
 * implementation through {@link InstanceProvider} without depending on it.
 *
 * @param <T> what is built
 * @param <S> the settings it is built from
 */
public interface Factory<T, S> {

    /**
     * Builds one instance from its settings.
     *
     * @param settings what to build it from
     * @return the new instance
     */
    T newInstance(S settings);
}
