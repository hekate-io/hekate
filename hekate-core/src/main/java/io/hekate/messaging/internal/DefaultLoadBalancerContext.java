/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologyHash;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.unicast.LoadBalancerContext;
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

    private final ClusterTopology topology;

    private final Optional<FailureInfo> failure;

    public DefaultLoadBalancerContext(MessageContext<?> ctx, ClusterTopology topology, Optional<FailureInfo> failure) {
        this(ctx.affinity(), ctx.affinityKey(), topology, failure);
    }

    public DefaultLoadBalancerContext(int affinity, Object affinityKey, ClusterTopology topology, Optional<FailureInfo> failure) {
        this.affinity = affinity;
        this.affinityKey = affinityKey;
        this.topology = topology;
        this.failure = failure;
    }

    @Override
    public ClusterTopology getTopology() {
        return topology;
    }

    @Override
    public boolean hasAffinity() {
        return affinityKey != null;
    }

    @Override
    public int getAffinity() {
        return affinity;
    }

    @Override
    public Object getAffinityKey() {
        return affinityKey;
    }

    @Override
    public Optional<FailureInfo> getFailure() {
        return failure;
    }

    @Override
    public long getVersion() {
        return topology.getVersion();
    }

    @Override
    public ClusterTopologyHash getHash() {
        return topology.getHash();
    }

    @Override
    public ClusterNode getLocalNode() {
        return topology.getLocalNode();
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return topology.getNodes();
    }

    @Override
    public List<ClusterNode> getNodesList() {
        return topology.getNodesList();
    }

    @Override
    public Set<ClusterNode> getRemoteNodes() {
        return topology.getRemoteNodes();
    }

    @Override
    public NavigableSet<ClusterNode> getJoinOrder() {
        return topology.getJoinOrder();
    }

    @Override
    public List<ClusterNode> getJoinOrderList() {
        return topology.getJoinOrderList();
    }

    @Override
    public NavigableSet<ClusterNode> getSorted() {
        return topology.getSorted();
    }

    @Override
    public List<ClusterNode> getSortedList() {
        return topology.getSortedList();
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
    public ClusterNode getOldest() {
        return topology.getOldest();
    }

    @Override
    public ClusterNode getYoungest() {
        return topology.getYoungest();
    }

    @Override
    public ClusterNode getRandom() {
        return topology.getRandom();
    }

    @Override
    public LoadBalancerContext filterAll(ClusterFilter filter) {
        ClusterTopology filtered = topology.filterAll(filter);

        if (filtered == topology) {
            return this;
        }

        return new DefaultLoadBalancerContext(affinity, affinityKey, filtered, failure);
    }

    @Override
    public LoadBalancerContext filter(ClusterNodeFilter filter) {
        ClusterTopology filtered = topology.filter(filter);

        if (filtered == topology) {
            return this;
        }

        return new DefaultLoadBalancerContext(affinity, affinityKey, filtered, failure);
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
