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

package io.hekate.metrics;

import io.hekate.metrics.local.MetricConfigBase;
import java.util.Map;

/**
 * Base interface for components that provide metrics information.
 */
public interface MetricsSource {
    /**
     * Returns the map of all available metrics. Returns an empty map if there are no metrics available.
     *
     * @return Map of metrics or an empty map.
     */
    Map<String, Metric> allMetrics();

    /**
     * Returns a metric for with the specified name or {@code null} if there is no such metric.
     *
     * @param name Metric name.
     *
     * @return Metric or {@code null}.
     */
    Metric metric(String name);

    /**
     * Returns the value of a metric or {@code defaultVal} if there is no such metric.
     *
     * @param name Metric name (see {@link MetricConfigBase#setName(String)}).
     * @param defaultVal Default value that should be returned if there is no such metric.
     *
     * @return Metric value or {@code defaultVal} if there is no such metric.
     */
    default long get(String name, long defaultVal) {
        Metric metric = metric(name);

        return metric != null ? metric.value() : defaultVal;
    }

    /**
     * Returns the value of a metric or 0 if there is no such metric.
     *
     * @param name Metric name (see {@link MetricConfigBase#setName(String)}).
     *
     * @return Metric value or 0 if there is no such metric.
     */
    default long get(String name) {
        return get(name, 0);
    }

    /**
     * Returns {@code true} if metric with the specified name exists.
     *
     * @param name Metric name.
     *
     * @return {@code true} if metric exists.
     */
    default boolean has(String name) {
        return metric(name) != null;
    }
}
