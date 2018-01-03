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

import io.hekate.metrics.Metric;
import io.hekate.metrics.local.MetricsUpdateEvent;
import io.hekate.util.format.ToString;
import java.util.Map;

class DefaultMetricsUpdateEvent implements MetricsUpdateEvent {
    private final int tick;

    private final Map<String, Metric> metrics;

    public DefaultMetricsUpdateEvent(int tick, Map<String, Metric> metrics) {
        assert metrics != null : "Metrics snapshot is null.";

        this.tick = tick;
        this.metrics = metrics;
    }

    @Override
    public int tick() {
        return tick;
    }

    @Override
    public Metric metric(String metric) {
        return metrics.get(metric);
    }

    @Override
    public Map<String, Metric> allMetrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return ToString.format(MetricsUpdateEvent.class, this);
    }
}
