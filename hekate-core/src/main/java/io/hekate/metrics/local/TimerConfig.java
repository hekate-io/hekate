/*
 * Copyright 2018 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.metrics.local;

import java.util.concurrent.TimeUnit;

/**
 * Configuration for {@link TimerMetric}.
 *
 * <p>
 * Fore more details about timers and other metrics please see the documentation of {@link LocalMetricsService}.
 * </p>
 *
 * @see LocalMetricsServiceFactory#withMetric(MetricConfigBase)
 * @see LocalMetricsService#register(TimerConfig)
 */
public class TimerConfig extends MetricConfigBase<TimerConfig> {
    private String rateName;

    private TimeUnit timeUnit = TimeUnit.NANOSECONDS;

    /**
     * Constructs new instance.
     */
    public TimerConfig() {
        // No-op.
    }

    /**
     * Constructs new instance with the specified name.
     *
     * @param name Name of this timer (see {@link #setName(String)}).
     */
    public TimerConfig(String name) {
        setName(name);
    }

    /**
     * Returns the name of a metric that will hold the rate value of this timer (see {@link #setRateName(String)}).
     *
     * @return Name of a metric that will hold the rate value of this timer.
     */
    public String getRateName() {
        return rateName;
    }

    /**
     * Sets the name of a metric that will hold the rate value of this timer. Can contain only alpha-numeric characters and
     * non-repeatable dots/hyphens.
     *
     * <p>
     * This parameter is optional. If specified then an additional metric of that name will be registered within the {@link
     * LocalMetricsService} and will hold the rate value of this timer.
     * </p>
     *
     * @param rateName Name of a metric that will hols the rate value of this timer (can contain only alpha-numeric characters and
     * non-repeatable dots/hyphens).
     */
    public void setRateName(String rateName) {
        this.rateName = rateName;
    }

    /**
     * Fluent-style version of {@link #setRateName(String)}.
     *
     * @param rateName Name of a metric that will hols the rate value of this timer (can contain only alpha-numeric characters and
     * non-repeatable dots/hyphens).
     *
     * @return This instance.
     */
    public TimerConfig withRateName(String rateName) {
        setRateName(rateName);

        return this;
    }

    /**
     * Returns the time unit of this timer (see {@link #setTimeUnit(TimeUnit)}).
     *
     * @return Time unit.
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * Sets the time unit of this timer.
     *
     * <p>
     * Default value of this parameter is {@link TimeUnit#NANOSECONDS}.
     * </p>
     *
     * @param timeUnit Time unit.
     */
    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit != null ? timeUnit : TimeUnit.NANOSECONDS;
    }

    /**
     * Fluent-style version of {@link #setTimeUnit(TimeUnit)}.
     *
     * @param timeUnit Time unit.
     *
     * @return This instance.
     */
    public TimerConfig withTimeUnit(TimeUnit timeUnit) {
        setTimeUnit(timeUnit);

        return this;
    }
}
