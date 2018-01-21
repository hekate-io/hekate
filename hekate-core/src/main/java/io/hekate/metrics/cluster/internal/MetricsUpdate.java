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

package io.hekate.metrics.cluster.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.metrics.MetricValue;
import io.hekate.util.format.ToString;
import java.util.Map;

class MetricsUpdate {
    private final ClusterNodeId node;

    private final long ver;

    private final Map<String, MetricValue> metrics;

    public MetricsUpdate(ClusterNodeId node, long ver, Map<String, MetricValue> metrics) {
        this.node = node;
        this.ver = ver;
        this.metrics = metrics;
    }

    public ClusterNodeId node() {
        return node;
    }

    public long version() {
        return ver;
    }

    public Map<String, MetricValue> metrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
