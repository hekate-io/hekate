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

import io.hekate.messaging.loadbalance.LoadBalancerContext;
import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Affinity key for RPC operations.
 *
 * <p>
 * This annotation can be placed on a parameter of an @{@link Rpc}-enabled interface's method. For each invocation of such RPC method,
 * value of that parameter will be used as {@link LoadBalancerContext#affinityKey() affinity key} for RPC request routing.
 * </p>
 *
 * <p>
 * Specifying the affinity key ensures that all RPC requests submitted with the same key will always be transmitted over the same network
 * connection and will always be processed by the same thread.
 * </p>
 *
 * <p>
 * {@link RpcLoadBalancer}  can also make use of the affinity key to perform consistent routing of messages among the cluster node. For
 * example, the default load balancer makes sure that all messages, having the same key, are always routed to the same node (unless
 * topology
 * doesn't change).
 * </p>
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface RpcAffinityKey {
    // No-op.
}
