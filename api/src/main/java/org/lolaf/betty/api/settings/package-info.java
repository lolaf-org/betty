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
/**
 * The settings objects a client or server is described by - sockets, buffer pools, IO threads and TLS - and the SPI
 * that turns a description into a running instance.
 * <p>
 * A settings class that can build what it configures implements
 * {@link org.lolaf.betty.api.settings.InstanceProvider}, which finds the single registered
 * {@link org.lolaf.betty.api.settings.Factory} through {@code ServiceLoader}. That is the whole of the seam between
 * api and impl: with only {@code betty-api} on the classpath the lookup finds nothing and
 * {@code newInstance()} throws.
 */
package org.lolaf.betty.api.settings;
