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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import java.util.Optional;
import java.util.Set;

public class GossipSpyAdaptor implements GossipListener {
    @Override
    public void onJoinReject(ClusterAddress rejectedBy, String reason) {
        // No-op.
    }

    @Override
    public void onStatusChange(GossipNodeStatus oldStatus, GossipNodeStatus newStatus, int order, Set<ClusterNode> topology) {
        // No-op.
    }

    @Override
    public void onTopologyChange(Set<ClusterNode> oldTopology, Set<ClusterNode> newTopology, Set<ClusterNode> failed) {
        // No-op.
    }

    @Override
    public void onKnownAddressesChange(Set<ClusterAddress> oldAddresses, Set<ClusterAddress> newAddresses) {
        // No-op.
    }

    @Override
    public void onNodeFailureSuspected(ClusterNode failed, GossipNodeStatus status) {
        // No-op.
    }

    @Override
    public void onNodeFailureUnsuspected(ClusterNode node, GossipNodeStatus status) {
        // No-op.
    }

    @Override
    public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
        // No-op.
    }

    @Override
    public void onNodeInconsistency(GossipNodeStatus status) {
        // No-op.
    }

    @Override
    public Optional<Throwable> onBeforeSend(GossipProtocol msg) {
        return Optional.empty();
    }
}
