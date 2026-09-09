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
 * The NIO implementation, reached through the api module's SPI and not meant to be referenced directly.
 * <p>
 * Almost everything here is package-private, and the four types that are not -
 * {@link org.lolaf.betty.impl.ExponentialMovingAverage}, {@link org.lolaf.betty.impl.FailSafeIOStats},
 * {@link org.lolaf.betty.impl.IOStatsScheduler} and
 * {@link org.lolaf.betty.impl.SharedIOBufferPoolInstanceRegistry} - have no consumer outside this package either.
 * They are documented because they ship in the javadoc jar, not because an application should name them: nothing
 * here is API, and any of it may change in any release.
 */
package org.lolaf.betty.impl;
