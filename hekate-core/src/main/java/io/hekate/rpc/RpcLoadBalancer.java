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

package io.hekate.rpc;

import io.hekate.messaging.MessagingService;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Client-side load balancer for RPC requests.
 *
 * <p>
 * This interface represents the RPC-specific version of a more generic {@link LoadBalancer} interface from the {@link MessagingService
 * messaging} API.
 * </p>
 *
 * <p>
 * Instances of this interface can be registered at configuration time via {@link RpcClientConfig#setLoadBalancer(RpcLoadBalancer)} or
 * dynamically at runtime via {@link RpcClientBuilder#withLoadBalancer(RpcLoadBalancer)}.
 * </p>
 *
 * <p>
 * For more details about load balancing please see the documentation of the {@link LoadBalancer} interface.
 * </p>
 *
 * @see RpcService
 * @see RpcRequest
 */
public interface RpcLoadBalancer extends LoadBalancer<RpcRequest> {
    // No-op.
}
