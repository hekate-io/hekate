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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;

/**
 * Implementation of {@link MetricFilter} that uses an exact match of a pre-defined string against a {@link Metric#name()}.
 */
public class MetricNameFilter implements MetricFilter {
    private final String metricName;

    /**
     * Constructs a new instance.
     *
     * @param metricName Metric name.
     */
    public MetricNameFilter(String metricName) {
        this.metricName = ArgAssert.notEmpty(metricName, "Metric name");
    }

    /**
     * Returns the metric name.
     *
     * @return Metric name.
     */
    public String metricName() {
        return metricName;
    }

    @Override
    public boolean accept(Metric metric) {
        return metricName.equals(metric.name());
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
