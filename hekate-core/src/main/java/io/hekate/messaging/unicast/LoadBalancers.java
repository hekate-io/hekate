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

package io.hekate.messaging.unicast;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.HekateException;

/**
 * Predefined load balancers.
 */
public final class LoadBalancers {
    private static class RandomLoadBalancer<T> implements LoadBalancer<T> {
        @Override
        public ClusterNodeId route(T message, LoadBalancerContext ctx) throws HekateException {
            ClusterNode target = ctx.getRandom();

            return target != null ? target.getId() : null;
        }
    }

    private static final LoadBalancer<?> RANDOM = new RandomLoadBalancer<>();

    private LoadBalancers() {
        // No-op.
    }

    /**
     * Routes all messages to randomly selected nodes.
     *
     * @param <T> Base type of messages.
     *
     * @return Load balancer.
     */
    @SuppressWarnings("unchecked")
    public static <T> LoadBalancer<T> toRandom() {
        return (LoadBalancer<T>)RANDOM;
    }
}
