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

package io.hekate.failover.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureResolution;
import io.hekate.util.format.ToString;
import java.util.Set;

public class DefaultFailoverContext implements FailoverContext {
    static class Resolution implements FailureResolution {
        private final boolean retry;

        private long delay;

        private FailoverRoutingPolicy routingPolicy = FailoverRoutingPolicy.PREFER_SAME_NODE;

        public Resolution(boolean retry) {
            this.retry = retry;
        }

        @Override
        public boolean isRetry() {
            return retry;
        }

        @Override
        public FailureResolution withDelay(long delay) {
            this.delay = delay;

            return this;
        }

        @Override
        public FailureResolution withReRoute() {
            routingPolicy = FailoverRoutingPolicy.RE_ROUTE;

            return this;
        }

        @Override
        public FailureResolution withSameNode() {
            routingPolicy = FailoverRoutingPolicy.RETRY_SAME_NODE;

            return this;
        }

        @Override
        public FailureResolution withSameNodeIfExists() {
            routingPolicy = FailoverRoutingPolicy.PREFER_SAME_NODE;

            return this;
        }

        @Override
        public FailureResolution withRoutingPolicy(FailoverRoutingPolicy routingPolicy) {
            ArgAssert.notNull(routingPolicy, "Routing policy");

            this.routingPolicy = routingPolicy;

            return this;
        }

        @Override
        public long delay() {
            return delay;
        }

        @Override
        public FailoverRoutingPolicy routing() {
            return routingPolicy;
        }

        @Override
        public String toString() {
            return ToString.format(FailureResolution.class, this);
        }
    }

    private final int attempt;

    private final Throwable error;

    private final ClusterNode failedNode;

    private final Set<ClusterNode> failedNodes;

    private final FailoverRoutingPolicy routing;

    public DefaultFailoverContext(int attempt, Throwable error, ClusterNode failedNode, Set<ClusterNode> failedNodes,
        FailoverRoutingPolicy routing) {
        assert attempt >= 0 : "Attempt must be >= 0";
        assert error != null : "Error is null.";
        assert failedNode != null : "Failed node is null.";
        assert failedNodes != null : "Failed nodes set is null.";
        assert !failedNodes.isEmpty() : "Failed nodes set is empty.";
        assert failedNodes.contains(failedNode) : "Failed nodes set doesn't contain the last failed node.";
        assert routing != null : "Failover routing policy is null.";

        this.attempt = attempt;
        this.error = error;
        this.failedNode = failedNode;
        this.failedNodes = failedNodes;
        this.routing = routing;
    }

    public DefaultFailoverContext withRouting(FailoverRoutingPolicy policy) {
        return new DefaultFailoverContext(attempt, error, failedNode, failedNodes, policy);
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public boolean isFirstAttempt() {
        return attempt == 0;
    }

    @Override
    public Throwable error() {
        return error;
    }

    @Override
    public ClusterNode failedNode() {
        return failedNode;
    }

    @Override
    public Set<ClusterNode> allFailedNodes() {
        return failedNodes;
    }

    @Override
    public boolean isFailed(ClusterNode node) {
        return failedNodes.contains(node);
    }

    @Override
    public FailoverRoutingPolicy routing() {
        return routing;
    }

    @Override
    public boolean isCausedBy(Class<? extends Throwable> type) {
        return ErrorUtils.isCausedBy(type, error);
    }

    @Override
    public FailureResolution fail() {
        return new Resolution(false);
    }

    @Override
    public FailureResolution retry() {
        return new Resolution(true);
    }

    @Override
    public String toString() {
        return ToString.format(FailoverContext.class, this);
    }
}
