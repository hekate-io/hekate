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

package io.hekate.cluster.health;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterUuid;
import io.hekate.core.HekateException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class FailureDetectorMock implements FailureDetector {
    public static class GlobalNodesState {
        private final Map<ClusterUuid, Set<ClusterUuid>> failedNodes = new HashMap<>();

        public void markFailed(ClusterUuid by, ClusterUuid id) {
            Set<ClusterUuid> suspectedBy = failedNodes.computeIfAbsent(id, k -> new HashSet<>());

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
    public long getHeartbeatInterval() {
        return 0;
    }

    @Override
    public int getFailureDetectionQuorum() {
        return 0;
    }

    @Override
    public void terminate() {
        // No-op.
    }

    @Override
    public boolean isAlive(ClusterAddress remote) {
        ClusterUuid id = remote.getId();

        return !globalState.failedNodes.containsKey(id) || !globalState.failedNodes.get(id).contains(node.getId());
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
}
