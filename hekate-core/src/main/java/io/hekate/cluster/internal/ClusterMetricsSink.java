/*
 * Copyright 2019 The Hekate Project
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.EnumMap;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicLong;

class ClusterMetricsSink {
    private final AtomicLong topologySize = new AtomicLong();

    private final AtomicLong topologyVersion = new AtomicLong();

    private final EnumMap<GossipProtocol.Type, Counter> counters;

    public ClusterMetricsSink(MeterRegistry metrics) {
        counters = new EnumMap<>(GossipProtocol.Type.class);

        Gauge.builder("hekate.cluster.topology_size", topologySize, AtomicLong::get).register(metrics);
        Gauge.builder("hekate.cluster.topology_version", topologyVersion, AtomicLong::get).register(metrics);

        register(GossipProtocol.Type.GOSSIP_UPDATE, metrics);
        register(GossipProtocol.Type.GOSSIP_UPDATE_DIGEST, metrics);
        register(GossipProtocol.Type.JOIN_REQUEST, metrics);
        register(GossipProtocol.Type.JOIN_ACCEPT, metrics);
        register(GossipProtocol.Type.JOIN_REJECT, metrics);
        register(GossipProtocol.Type.HEARTBEAT_REQUEST, metrics);
        register(GossipProtocol.Type.HEARTBEAT_REPLY, metrics);
    }

    public void onGossipMessage(GossipProtocol.Type type) {
        Counter counter = counters.get(type);

        if (counter != null) {
            counter.increment();
        }
    }

    public void onTopologyChange(ClusterTopology topology) {
        topologySize.set(topology.size());
        topologyVersion.set(topology.version());
    }

    private void register(GossipProtocol.Type type, MeterRegistry metrics) {
        Counter counter = Counter.builder("hekate.cluster.gossip_messages")
            .tag("type", type.name().toLowerCase(Locale.ENGLISH))
            .register(metrics);

        counters.put(type, counter);
    }
}
