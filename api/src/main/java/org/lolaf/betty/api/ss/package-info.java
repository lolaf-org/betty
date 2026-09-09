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
 * How an IO thread waits for its selector, which is the single biggest latency-versus-CPU choice in betty.
 * <p>
 * {@link org.lolaf.betty.api.ss.WakeupSelectStrategy} blocks and gives the core back, paying a wakeup syscall for
 * every cross-thread write; {@link org.lolaf.betty.api.ss.IdleStrategySelectStrategy} polls and pays a core.
 * A strategy belongs to an {@code IOWorkersGroupSettings.IOThreadGroup} rather than to a whole group, so one
 * client or server can spend a core on the thread group carrying the sessions that matter and block on the rest.
 * {@code WakeupSelectStrategy} with a 10 ms timeout is what a thread group gets when none is set.
 */
package org.lolaf.betty.api.ss;
