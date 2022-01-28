/*
 * Copyright 2022 The Hekate Project
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
    public String toString() {
        return ToString.format(FailedAttempt.class, this);
    }
}
