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
import io.hekate.metrics.MetricValue;
import io.hekate.metrics.cluster.ClusterNodeMetrics;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.Collections.unmodifiableMap;

class MetricsReplica {
    private final ClusterNode node;

    private final ReentrantLock lock = new ReentrantLock();

    private long version;

    private Map<String, MetricValue> metrics;

    private volatile Optional<ClusterNodeMetrics> publicMetrics = Optional.empty();

    public MetricsReplica(ClusterNode node) {
        this.node = node;
    }

    public ClusterNode node() {
        return node;
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public long version() {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock on " + MetricsReplica.class.getSimpleName();

        return version;
    }

    public Map<String, MetricValue> metrics() {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock on " + MetricsReplica.class.getSimpleName();

        return metrics;
    }

    public void update(long version, Map<String, MetricValue> metrics) {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock on " + MetricsReplica.class.getSimpleName();

        this.version = version;
        this.metrics = metrics;

        publicMetrics = Optional.of(new DefaultClusterNodeMetrics(node(), unmodifiableMap(new HashMap<>(metrics))));
    }

    public Optional<ClusterNodeMetrics> publicMetrics() {
        // Volatile read.
        return publicMetrics;
    }
}
