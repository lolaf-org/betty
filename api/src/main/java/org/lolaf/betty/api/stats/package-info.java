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
 * What betty measures: per session ({@link org.lolaf.betty.api.stats.IOStats}), per IO thread
 * ({@link org.lolaf.betty.api.stats.IOWorkerStats}) and per buffer pool
 * ({@link org.lolaf.betty.api.stats.IOBufferPoolStats}).
 * <p>
 * These are interfaces the application implements, not counters betty keeps - so where the numbers go, and what
 * they cost, is the application's decision. Measurement is genuinely free when it is off: betty reads its clock
 * through {@code IOStats.getTimeInNanos}, so an implementation that does not time an operation returns a constant
 * and no clock is ever read for it.
 */
package org.lolaf.betty.api.stats;
