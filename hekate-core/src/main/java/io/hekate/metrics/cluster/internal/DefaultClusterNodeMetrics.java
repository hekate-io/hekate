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

import io.hekate.cluster.ClusterNode;
import io.hekate.metrics.Metric;
import io.hekate.metrics.cluster.ClusterNodeMetrics;
import io.hekate.util.format.ToString;
import java.util.Map;

class DefaultClusterNodeMetrics implements ClusterNodeMetrics {
    private final ClusterNode node;

    private final Map<String, Metric> metrics;

    public DefaultClusterNodeMetrics(ClusterNode node, Map<String, Metric> metrics) {
        this.node = node;
        this.metrics = metrics;
    }

    @Override
    public ClusterNode node() {
        return node;
    }

    @Override
    public Metric metric(String name) {
        return metrics.get(name);
    }

    @Override
    public Map<String, Metric> allMetrics() {
        return metrics;
    }

    @Override
    public String toString() {
        return ToString.format(ClusterNodeMetrics.class, this);
    }
}
