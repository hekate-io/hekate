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
import io.hekate.messaging.retry.FailedAttempt;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static io.hekate.messaging.retry.RetryRoutingPolicy.RE_ROUTE;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;
import static java.util.stream.Collectors.toList;

/**
 * Common load balancers.
 */
public final class LoadBalancers {
    private static class Random<T> implements LoadBalancer<T> {
        @Override
        public ClusterNodeId route(T msg, LoadBalancerContext ctx) {
            ClusterNode target = ctx.random();

            return target != null ? target.id() : null;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    private static class RoundRobin<T> implements LoadBalancer<T> {
        private static final AtomicIntegerFieldUpdater<RoundRobin> COUNTER = newUpdater(RoundRobin.class, "counter");

        @SuppressWarnings("unused") // <-- Accessed via AtomicIntegerFieldUpdater.
        private volatile int counter;

        @Override
        public ClusterNodeId route(T msg, LoadBalancerContext ctx) {
            assert !ctx.topology().isEmpty() : "Topology is empty.";

            int idx = COUNTER.getAndUpdate(this, val -> {
                int newVal = val + 1;

                return newVal >= ctx.size() ? 0 : newVal;
            });

            ClusterNode node = ctx.nodes().get(idx);

            if (ctx.failure().isPresent()) {
                FailedAttempt failure = ctx.failure().get();

                if (failure.routing() == RE_ROUTE && failure.hasTriedNode(node)) {
                    List<ClusterNode> nonFailed = ctx.stream()
                        .filter(n -> !failure.hasTriedNode(n))
                        .collect(toList());

                    if (!nonFailed.isEmpty()) {
                        Collections.shuffle(nonFailed);

                        node = nonFailed.get(0);
                    }
                }
            }

            return node.id();
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }
    }

    private static final LoadBalancer<?> RANDOM = new Random<>();

    private LoadBalancers() {
        // No-op.
    }

    /**
     * Routes each message to a to randomly selected node.
     *
     * @param <T> Base type of messages.
     *
     * @return Load balancer.
     */
    @SuppressWarnings("unchecked")
    public static <T> LoadBalancer<T> random() {
        return (LoadBalancer<T>)RANDOM;
    }

    /**
     * Returns a new load balancer that routes all messages using a round-robin approach.
     *
     * @param <T> Base type of messages.
     *
     * @return Load balancer.
     */
    @SuppressWarnings("unchecked")
    public static <T> LoadBalancer<T> newRoundRobin() {
        return (LoadBalancer<T>)new RoundRobin<>();
    }
}
