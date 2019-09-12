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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.messaging.retry.RetryRoutingPolicy;
import io.hekate.util.format.ToString;
import java.util.Set;

class MessageOperationFailure implements FailedAttempt {
    private final int attempt;

    private final Throwable error;

    private final ClusterNode lastTriedNode;

    private final Set<ClusterNode> allTriedNodes;

    private final RetryRoutingPolicy routing;

    public MessageOperationFailure(
        int attempt,
        Throwable error,
        ClusterNode lastTriedNode,
        Set<ClusterNode> triedNodes,
        RetryRoutingPolicy routing
    ) {
        assert attempt >= 0 : "Attempt must be >= 0";
        assert error != null : "Error is null.";
        assert lastTriedNode != null : "Last tried node is null.";
        assert triedNodes != null : "Tried node set is null.";
        assert !triedNodes.isEmpty() : "Tried node set is empty.";
        assert triedNodes.contains(lastTriedNode) : "Tried node set doesn't contain the last tried node.";
        assert routing != null : "Retry routing policy is null.";

        this.attempt = attempt;
        this.error = error;
        this.lastTriedNode = lastTriedNode;
        this.allTriedNodes = triedNodes;
        this.routing = routing;
    }

    public MessageOperationFailure withRouting(RetryRoutingPolicy policy) {
        return new MessageOperationFailure(attempt, error, lastTriedNode, allTriedNodes, policy);
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public Throwable error() {
        return error;
    }

    @Override
    public ClusterNode lastTriedNode() {
        return lastTriedNode;
    }

    @Override
    public Set<ClusterNode> allTriedNodes() {
        return allTriedNodes;
    }

    @Override
    public RetryRoutingPolicy routing() {
        return routing;
    }

    @Override
    public boolean isCausedBy(Class<? extends Throwable> type) {
        return ErrorUtils.isCausedBy(type, error);
    }

    @Override
    public String toString() {
        return ToString.format(FailedAttempt.class, this);
    }
}
