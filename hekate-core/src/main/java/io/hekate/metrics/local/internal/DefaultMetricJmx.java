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

package io.hekate.metrics.local.internal;

import io.hekate.metrics.MetricJmx;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class DefaultMetricJmx implements MetricJmx {
    private final String name;

    @ToStringIgnore
    private final LocalMetricsService metrics;

    public DefaultMetricJmx(String name, LocalMetricsService metrics) {
        assert name != null : "Metric name is null.";
        assert metrics != null : "Metrics service is null.";

        this.name = name;
        this.metrics = metrics;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getValue() {
        return metrics.snapshot().get(name);
    }

    @Override
    public String toString() {
        return ToString.format(MetricJmx.class, this);
    }
}
