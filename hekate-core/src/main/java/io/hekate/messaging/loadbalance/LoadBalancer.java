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

import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingService;

/**
 * Load balancer for unicast messaging.
 *
 * <p>
 * Implementations of this interface are responsible for unicast message routing within a {@link MessagingChannel}. Channel calls the
 * {@link #route(Object, LoadBalancerContext)} method of this interface every time when it is going to perform a unicast operation
 * (f.e. {@link MessagingChannel#newSend(Object) send(...)} or {@link MessagingChannel#newRequest(Object) request(...)}).
 * Implementations of this method must select one of the cluster nodes from the provided context. The selected cluster node will be used by
 * the channel as a messaging operation target.
 * </p>
 *
 * <p>
 * Below is the example that uses a modulo-based approach for load balancing:
 * ${source: messaging/loadbalance/LoadBalancerJavadocTest.java#load_balancer}
 * </p>
 *
 * <p>
 * Instances of this interface can be registered via {@link MessagingChannel#withLoadBalancer(LoadBalancer)} or
 * {@link MessagingChannelConfig#setLoadBalancer(LoadBalancer)} methods.
 * </p>
 *
 * @param <T> Base type of messages that can be handled by this load balancer.
 *
 * @see MessagingService
 * @see MessagingChannel#withLoadBalancer(LoadBalancer)
 */
@FunctionalInterface
public interface LoadBalancer<T> {
    /**
     * Selects one of the cluster nodes from the load balancer context. Note that the provided context contains only those nodes that are
     * capable of receiving messages of this channel (i.e. have {@link MessageReceiver}) and match the channel's
     * {@link MessagingChannel#filter(ClusterNodeFilter) topology filtering} rules.
     *
     * <p>
     * Returning {@code null} from this method (if target node can't be selected for some reason) will cause message sending operation to
     * fail with {@link LoadBalancerException}.
     * </p>
     *
     * @param msg Message.
     * @param ctx Load balancer context.
     *
     * @return Node that should be used for the messaging operation.
     *
     * @throws LoadBalancerException if failed to perform load balancing.
     */
    ClusterNodeId route(T msg, LoadBalancerContext ctx) throws LoadBalancerException;
}
