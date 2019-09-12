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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.partition.PartitionMapper;
import io.hekate.util.format.ToString;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

class DefaultLoadBalancerContext implements LoadBalancerContext {
    private final int affinity;

    private final Object affinityKey;

    private final PartitionMapper partitions;

    private final ClusterTopology topology;

    private final Optional<FailedAttempt> failure;

    public DefaultLoadBalancerContext(
        int affinity,
        Object affinityKey,
        ClusterTopology topology,
        PartitionMapper partitions,
        Optional<FailedAttempt> failure
    ) {
        this.affinity = affinity;
        this.affinityKey = affinityKey;
        this.topology = topology;
        this.partitions = partitions;
        this.failure = failure;
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    @Override
    public PartitionMapper partitions() {
        return partitions;
    }

    @Override
    public boolean hasAffinity() {
        return affinityKey != null;
    }

    @Override
    public int affinity() {
        return affinity;
    }

    @Override
    public Object affinityKey() {
        return affinityKey;
    }

    @Override
    public Optional<FailedAttempt> failure() {
        return failure;
    }

    @Override
    public long version() {
        return topology.version();
    }

    @Override
    public ClusterHash hash() {
        return topology.hash();
    }

    @Override
    public ClusterNode localNode() {
        return topology.localNode();
    }

    @Override
    public List<ClusterNode> nodes() {
        return topology.nodes();
    }

    @Override
    public ClusterNode first() {
        return topology.first();
    }

    @Override
    public ClusterNode last() {
        return topology.last();
    }

    @Override
    public Set<ClusterNode> nodeSet() {
        return topology.nodeSet();
    }

    @Override
    public List<ClusterNode> remoteNodes() {
        return topology.remoteNodes();
    }

    @Override
    public NavigableSet<ClusterNode> joinOrder() {
        return topology.joinOrder();
    }

    @Override
    public Stream<ClusterNode> stream() {
        return topology.stream();
    }

    @Override
    public boolean contains(ClusterNode node) {
        return topology.contains(node);
    }

    @Override
    public boolean contains(ClusterNodeId node) {
        return topology.contains(node);
    }

    @Override
    public ClusterNode get(ClusterNodeId id) {
        return topology.get(id);
    }

    @Override
    public int size() {
        return topology.size();
    }

    @Override
    public boolean isEmpty() {
        return topology.isEmpty();
    }

    @Override
    public ClusterNode oldest() {
        return topology.oldest();
    }

    @Override
    public ClusterNode youngest() {
        return topology.youngest();
    }

    @Override
    public ClusterNode random() {
        return topology.random();
    }

    @Override
    public LoadBalancerContext filterAll(ClusterFilter filter) {
        ClusterTopology filtered = topology.filterAll(filter);

        if (filtered == topology) {
            return this;
        }

        return new DefaultLoadBalancerContext(affinity, affinityKey, filtered, partitions, failure);
    }

    @Override
    public LoadBalancerContext filter(ClusterNodeFilter filter) {
        ClusterTopology filtered = topology.filter(filter);

        if (filtered == topology) {
            return this;
        }

        return new DefaultLoadBalancerContext(affinity, affinityKey, filtered, partitions, failure);
    }

    @Override
    public Iterator<ClusterNode> iterator() {
        return topology.iterator();
    }

    @Override
    public String toString() {
        return ToString.format(LoadBalancerContext.class, this);
    }
}
