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

package io.hekate.failover.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Utils;
import io.hekate.failover.FailoverContext;
import io.hekate.failover.FailoverRoutingPolicy;
import io.hekate.failover.FailureResolution;
import io.hekate.util.format.ToString;
import java.util.Optional;
import java.util.Set;

public class DefaultFailoverContext implements FailoverContext {
    static class Resolution implements FailureResolution {
        private final boolean retry;

        private long delay;

        private FailoverRoutingPolicy routingPolicy = FailoverRoutingPolicy.RETRY_SAME_NODE;

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
            ArgAssert.check(routingPolicy != null, "Routing policy is null.");

            this.routingPolicy = routingPolicy;

            return this;
        }

        @Override
        public long getDelay() {
            return delay;
        }

        @Override
        public FailoverRoutingPolicy getRoutingPolicy() {
            return routingPolicy;
        }

        @Override
        public String toString() {
            return ToString.format(FailureResolution.class, this);
        }
    }

    private final int attempt;

    private final Throwable error;

    private final Optional<ClusterNode> lastNode;

    private final Set<ClusterNode> failedNodes;

    private final FailoverRoutingPolicy routing;

    public DefaultFailoverContext(int attempt, Throwable error, Optional<ClusterNode> lastNode, Set<ClusterNode> failedNodes,
        FailoverRoutingPolicy routing) {
        assert attempt >= 0 : "Attempt must be >= 0";
        assert error != null : "Error is null.";
        assert lastNode != null : "Last tried node is null.";
        assert failedNodes != null : "Failed nodes set is null.";
        assert routing != null : "Failover routing policy is null.";

        this.attempt = attempt;
        this.error = error;
        this.lastNode = lastNode;
        this.failedNodes = failedNodes;
        this.routing = routing;
    }

    public DefaultFailoverContext withRouting(FailoverRoutingPolicy policy) {
        return new DefaultFailoverContext(attempt, error, lastNode, failedNodes, policy);
    }

    @Override
    public int getAttempt() {
        return attempt;
    }

    @Override
    public boolean isFirstAttempt() {
        return attempt == 0;
    }

    @Override
    public Throwable getError() {
        return error;
    }

    @Override
    public Optional<ClusterNode> getLastNode() {
        return lastNode;
    }

    @Override
    public Set<ClusterNode> getFailedNodes() {
        return failedNodes;
    }

    @Override
    public FailoverRoutingPolicy getRouting() {
        return routing;
    }

    @Override
    public boolean isCausedBy(Class<? extends Throwable> type) {
        return Utils.isCausedBy(error, type);
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
