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

import lombok.Getter;
import org.lolaf.ringos.Deadline;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Something with a start and a stop, returning itself so a call can be chained onto its construction.
 * <p>
 * Both calls are idempotent in every implementation here: starting a started instance, or stopping a stopped one,
 * does nothing rather than failing.
 *
 * @param <T> the implementing type, so {@link #start()} and {@link #stop(Deadline)} return it rather than
 *            {@code Startable}
 */
public interface Startable<T> {

    /**
     * Stops, giving work already in flight until the deadline to finish; what is unfinished then is abandoned.
     *
     * @param stopDeadline how long the orderly part of the shutdown may take
     * @return this instance
     * @throws StartStopException if stopping fails
     */
    T stop(Deadline stopDeadline) throws StartStopException;

    /**
     * Starts, doing nothing if already started.
     *
     * @return this instance
     * @throws StartStopException if starting fails
     */
    T start() throws StartStopException;

    /**
     * Whether this instance is started.
     *
     * @return true between a successful {@link #start()} and the {@link #stop(Deadline)} that follows it
     */
    boolean isStarted();

    /**
     * Unchecked so that a lifecycle call reads as one statement; a failure to start or stop is not something a
     * caller can usefully recover from part-way.
     */
    class StartStopException extends RuntimeException {

        /**
         * A failure described by a message alone.
         *
         * @param message what went wrong
         */
        public StartStopException(String message) {
            super(message);
        }

        /**
         * A failure carrying the exception that caused it.
         *
         * @param cause what went wrong
         */
        public StartStopException(Throwable cause) {
            super(cause);
        }

        /**
         * A failure with both a message and the exception that caused it.
         *
         * @param message what went wrong
         * @param cause   the exception behind it
         */
        public StartStopException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Tracks the flag and does nothing else, for an implementation that has nothing to start.
     * <p>
     * The flag is a plain field: a no-op lifecycle is not worth the memory barrier, so start and stop must come
     * from one thread.
     *
     * @param <T> the implementing type
     */
    @Getter
    abstract class VoidStartable<T> implements Startable<T> {

        private boolean started;

        @Override
        public final T stop(Deadline stopDeadline) throws StartStopException {
            started = false;
            return (T) this;
        }

        @Override
        public final T start() throws StartStopException {
            started = true;
            return (T) this;
        }
    }

    /**
     * Makes the lifecycle idempotent and thread-safe once, leaving the subclass only the work.
     * <p>
     * The flag moves before {@link #startMe()} runs, so a concurrent caller does not start twice; a
     * {@code startMe} that throws puts it back, so the failure can be retried.
     *
     * @param <T> the implementing type
     */
    @Getter
    abstract class SimpleStartable<T> implements Startable<T> {

        private final AtomicBoolean started = new AtomicBoolean(false);

        @Override
        public boolean isStarted() {
            return started.get();
        }

        @Override
        public final T stop(Deadline stopDeadline) throws StartStopException {
            if (started.getAndSet(false)) {
                stopMe(stopDeadline);
            }
            return (T) this;
        }

        /**
         * Called once per stop, only from the caller that observed the instance as started.
         *
         * @param stopDeadline how long the orderly part of the shutdown may take
         * @throws StartStopException if stopping fails
         */
        protected abstract void stopMe(Deadline stopDeadline) throws StartStopException;

        @Override
        public final T start() throws StartStopException {
            if (!started.getAndSet(true)) {
                try {
                    startMe();
                } catch (Exception ex) {
                    started.set(false);
                    throw ex;
                }
            }
            return (T) this;
        }

        /**
         * Called once per start, only from the caller that observed the instance as stopped.
         *
         * @throws StartStopException if starting fails
         */
        protected abstract void startMe() throws StartStopException;
    }


}
