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

import io.hekate.metrics.Metric;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Metric for tracking the duration of an arbitrary event.
 *
 * <p>
 * Timers can be registered within the {@link LocalMetricsService} either at
 * {@link LocalMetricsServiceFactory#withMetric(MetricConfigBase) configuration time} or {@link LocalMetricsService#register(TimerConfig)
 * dynamically} at runtime. Once registered they can be accessed via the {@link LocalMetricsService#timer(String)} method.
 * </p>
 *
 * <p>
 * For more details about metric types and their usage please see the documentation of {@link LocalMetricsService} interface.
 * </p>
 *
 * @see LocalMetricsService
 * @see TimerConfig
 */
public interface TimerMetric extends Metric {
    /**
     * Returns a new {@link TimeSpan}.
     *
     * @return Time span.
     */
    TimeSpan start();

    /**
     * Returns {@code true} if this timer has a {@link TimerConfig#setRateName(String) rate metric}.
     *
     * @return {@code true} if this timer has a {@link TimerConfig#setRateName(String) rate metric}.
     *
     * @see #rate()
     */
    boolean hasRate();

    /**
     * Returns the {@link TimerConfig#setRateName(String) rate metric} of this timer or {@code null} if this timer doesn't have a
     * rate metric.
     *
     * @return Metric or {@code null}.
     *
     * @see #hasRate()
     */
    Metric rate();

    /**
     * Returns the {@link TimerConfig#setTimeUnit(TimeUnit) time unit }of this timer.
     *
     * @return Time unit.
     */
    TimeUnit timeUnit();

    /**
     * Measures the duration of the specified {@link Callable} task.
     *
     * @param task Task to measure.
     * @param <V> Type of a value returned by the task.
     *
     * @return Task execution result (see {@link Callable#call()}).
     *
     * @throws Exception Task execution failure.
     */
    default <V> V measure(Callable<V> task) throws Exception {
        try (TimeSpan ignore = start()) {
            return task.call();
        }
    }

    /**
     * Measures the duration of the specified {@link Runnable} task.
     *
     * @param task Task to measure.
     */
    default void measure(Runnable task) {
        try (TimeSpan ignore = start()) {
            task.run();
        }
    }
}
