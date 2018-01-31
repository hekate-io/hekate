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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.internal.gossip.GossipProtocol;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import java.util.EnumMap;

public class ClusterMetricsSink {
    private final EnumMap<GossipProtocol.Type, CounterMetric> counters;

    private final CounterMetric topologySize;

    private final CounterMetric topologyVersion;

    public ClusterMetricsSink(LocalMetricsService service) {
        counters = new EnumMap<>(GossipProtocol.Type.class);

        topologySize = service.register(new CounterConfig("hekate.cluster.topology.size"));
        topologyVersion = service.register(new CounterConfig("hekate.cluster.topology.version"));

        register(GossipProtocol.Type.GOSSIP_UPDATE, "hekate.cluster.gossip.update", service);
        register(GossipProtocol.Type.GOSSIP_UPDATE_DIGEST, "hekate.cluster.gossip.digest", service);
        register(GossipProtocol.Type.JOIN_REQUEST, "hekate.cluster.gossip.join-request", service);
        register(GossipProtocol.Type.JOIN_ACCEPT, "hekate.cluster.gossip.join-accept", service);
        register(GossipProtocol.Type.JOIN_REJECT, "hekate.cluster.gossip.join-reject", service);
        register(GossipProtocol.Type.HEARTBEAT_REQUEST, "hekate.cluster.gossip.hb-request", service);
        register(GossipProtocol.Type.HEARTBEAT_REPLY, "hekate.cluster.gossip.hb-response", service);
    }

    public void onGossipMessage(GossipProtocol.Type type) {
        CounterMetric counter = counters.get(type);

        if (counter != null) {
            counter.increment();
        }
    }

    public void onTopologyChange(ClusterTopology topology) {
        long sizeDiff = topology.size() - topologySize.value();
        long verDiff = topology.version() - topologyVersion.value();

        topologySize.add(sizeDiff);
        topologyVersion.add(verDiff);
    }

    private void register(GossipProtocol.Type type, String name, LocalMetricsService service) {
        CounterMetric counter = service.register(new CounterConfig(name).withAutoReset(true));

        counters.put(type, counter);
    }
}
