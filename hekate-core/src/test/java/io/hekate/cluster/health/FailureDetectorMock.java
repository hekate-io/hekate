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

package io.hekate.cluster.health;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.HekateException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FailureDetectorMock implements FailureDetector {
    public static class GlobalNodesState {
        private final Map<ClusterNodeId, Set<ClusterNodeId>> failedNodes = new HashMap<>();

        public void markFailed(ClusterNodeId by, ClusterNodeId id) {
            Set<ClusterNodeId> suspectedBy = failedNodes.computeIfAbsent(id, k -> new HashSet<>());

            suspectedBy.add(by);
        }
    }

    private final ClusterNode node;

    private final GlobalNodesState globalState;

    public FailureDetectorMock(ClusterNode node, GlobalNodesState globalState) {
        this.node = node;
        this.globalState = globalState;
    }

    @Override
    public void initialize(FailureDetectorContext context) throws HekateException {
        // No-op.
    }

    @Override
    public long heartbeatInterval() {
        return 0;
    }

    @Override
    public int failureQuorum() {
        return 0;
    }

    @Override
    public void terminate() {
        // No-op.
    }

    @Override
    public boolean isAlive(ClusterAddress remote) {
        ClusterNodeId id = remote.id();

        return !globalState.failedNodes.containsKey(id) || !globalState.failedNodes.get(id).contains(node.id());
    }

    @Override
    public void update(Set<ClusterAddress> nodes) {
        // No-op.
    }

    @Override
    public Collection<ClusterAddress> heartbeatTick() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean onHeartbeatRequest(ClusterAddress from) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onHeartbeatReply(ClusterAddress node) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onConnectFailure(ClusterAddress node) {
        throw new UnsupportedOperationException();
    }
}
