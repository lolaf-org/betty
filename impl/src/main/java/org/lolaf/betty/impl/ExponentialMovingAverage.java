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

import lombok.Getter;

import java.time.Duration;

/**
 * Exponential Moving Average (EMA) calculator for process load monitoring.
 * <p>
 * The EMA gives more weight to recent values while still considering historical data.
 * Formula: EMA_new = alpha * current_value + (1 - alpha) * EMA_old
 */
public class ExponentialMovingAverage {

    private final double alpha;
    // written by the stats thread, read by whichever thread places or rebalances a connection; single-writer, so
    // volatile is enough, and it also stops a non-atomic double write being read in halves
    @Getter
    private volatile double ema;
    private volatile boolean initialized;

    /**
     * Creates an EMA with the specified smoothing factor.
     *
     * @param alpha Smoothing factor between 0 and 1
     *              - Higher values (0.3-0.5) = more responsive to recent changes
     *              - Lower values (0.01-0.1) = smoother, less reactive
     */
    public ExponentialMovingAverage(double alpha) {
        if (alpha <= 0 || alpha > 1) {
            throw new IllegalArgumentException("Alpha must be between 0 and 1");
        }
        this.alpha = alpha;
        this.initialized = false;
    }

    /**
     * Creates an EMA based on a time window and update interval.
     * <p>
     * For example, if you want a 5-minute window and update every 10 seconds:
     * fromTimeWindow(300000, 10000) or fromTimeWindow(5*60, 10) with seconds
     * <p>
     * This calculates the number of samples in the window and converts to alpha.
     *
     * @param timeWindow     The time window to average over
     * @param updateInterval How often the EMA is updated
     * @return An EMA configured for the specified time window
     */
    public static ExponentialMovingAverage fromTimeWindow(Duration timeWindow, Duration updateInterval) {
        if (updateInterval.toMillis() > timeWindow.toMillis()) {
            throw new IllegalArgumentException("Update interval cannot be larger than time window");
        }

        int period = (int) Math.ceil((double) timeWindow.toMillis() / updateInterval.toMillis());
        return fromPeriod(period);
    }

    /**
     * Creates an EMA based on a time period (number of samples).
     * Converts period N to alpha using: alpha = 2 / (N + 1)
     *
     * @param period Number of periods (e.g., 10 for a 10-sample EMA)
     * @return An EMA weighted over that many samples
     */
    public static ExponentialMovingAverage fromPeriod(int period) {
        if (period < 1) {
            throw new IllegalArgumentException("Period must be at least 1");
        }
        double alpha = 2.0 / (period + 1);
        return new ExponentialMovingAverage(alpha);
    }

    /**
     * Folds one sample in. The first sample becomes the average outright rather than being pulled up from zero,
     * so a fresh EMA does not spend its first window reading low.
     *
     * @param value the new sample
     * @return the updated average, also available from {@code getEma()}
     */
    public double update(double value) {
        if (!initialized) {
            ema = value;
            initialized = true;
        } else {
            ema = alpha * value + (1 - alpha) * ema;
        }
        return ema;
    }

    /**
     * Forgets everything, so the next {@link #update(double)} starts the average again.
     */
    public void reset() {
        ema = 0;
        initialized = false;
    }
}