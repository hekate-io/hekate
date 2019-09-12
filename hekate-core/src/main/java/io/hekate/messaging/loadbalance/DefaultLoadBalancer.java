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

package io.hekate.messaging.loadbalance;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.retry.FailedAttempt;
import io.hekate.partition.Partition;
import java.util.Collections;
import java.util.List;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RE_ROUTE;
import static java.util.stream.Collectors.toList;

/**
 * Default implementation of the {@link LoadBalancer} interface.
 *
 * <p>
 * This load balancer uses {@link #nonAffinityRoute(Object, LoadBalancerContext)} and {@link #affinityRoute(Object, LoadBalancerContext)}
 * methods to perform actual load balancing. Which of those methods will be used depends on whether the load balanced operation has an
 * {@link LoadBalancerContext#affinityKey() affinity key} or not. Please see the documentation of those methods for more details about their
 * logic.
 * </p>
 *
 * @param <T> Base type of messages that can be handled by this load balancer.
 */
public class DefaultLoadBalancer<T> implements LoadBalancer<T> {
    @Override
    public ClusterNodeId route(T msg, LoadBalancerContext ctx) throws LoadBalancerException {
        ClusterNode selected;

        if (ctx.hasAffinity()) {
            selected = affinityRoute(msg, ctx);

        } else {
            selected = nonAffinityRoute(msg, ctx);

        }

        return selected != null ? selected.id() : null;
    }

    /**
     * Selects a {@link LoadBalancerContext#random() random} node from the load balancer context. If the selected node is known
     * to be {@link LoadBalancerContext#failure() failed} then another random non-failed node will be selected. If all nodes are known to be
     * failed then this method will fallback to the initially selected node.
     *
     * @param msg Message.
     * @param ctx Load balancer context.
     *
     * @return Selected node.
     *
     * @throws LoadBalancerException if failed to perform load balancing.
     */
    protected ClusterNode nonAffinityRoute(T msg, LoadBalancerContext ctx) throws LoadBalancerException {
        // Select any random node.
        ClusterNode selected = ctx.random();

        // Check if this is a retry attempt and try to re-route in case if the selected node is known to be failed.
        if (ctx.failure().isPresent()) {
            FailedAttempt failure = ctx.failure().get();

            if (failure.routing() == RE_ROUTE && failure.hasTriedNode(selected)) {
                // Exclude all failed nodes.
                List<ClusterNode> nonFailed = ctx.stream()
                    .filter(n -> !failure.hasTriedNode(n))
                    .collect(toList());

                if (!nonFailed.isEmpty()) {
                    // Randomize.
                    Collections.shuffle(nonFailed);

                    selected = nonFailed.get(0);
                }
            }
        }

        return selected;
    }

    /**
     * Selects a node based on the {@link LoadBalancerContext#partitions() partition} mapping of the operation's
     * {@link LoadBalancerContext#affinityKey() affinity key}.
     *
     * <p>
     * This method tries to use the {@link Partition#primaryNode() primary node} of the selected {@link Partition}. If  the selected node
     * is known to be {@link LoadBalancerContext#failure() failed} then a {@link Partition#backupNodes() backup node} will be selected. If
     * partition doesn't have any backup nodes or if it is known that all backup nodes failed then this method will fallback to the
     * initially selected node.
     * </p>
     *
     * @param msg Message.
     * @param ctx Load balancer context.
     *
     * @return Selected node.
     *
     * @throws LoadBalancerException if failed to perform load balancing.
     */
    protected ClusterNode affinityRoute(T msg, LoadBalancerContext ctx) throws LoadBalancerException {
        ArgAssert.isTrue(ctx.hasAffinity(), "Can't load balance on non-affinity context.");

        // Use partition-based routing.
        Partition partition = ctx.partitions().mapInt(ctx.affinity());

        ClusterNode selected = partition.primaryNode();

        // Check if this is a retry attempt and try to re-route in case if the selected node is known to be failed.
        if (ctx.failure().isPresent() && partition.hasBackupNodes()) {
            FailedAttempt failure = ctx.failure().get();

            if (failure.routing() == RE_ROUTE && failure.hasTriedNode(selected)) {
                selected = partition.backupNodes().stream()
                    .filter(n -> !failure.hasTriedNode(n))
                    .findFirst()
                    .orElse(selected); // <-- Fall back to the originally selected node.
            }
        }

        return selected;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
