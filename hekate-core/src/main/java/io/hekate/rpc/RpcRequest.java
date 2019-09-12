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

import java.lang.reflect.Method;

/**
 * RPC request for load balancing.
 *
 * @see RpcLoadBalancer
 */
public interface RpcRequest {
    /**
     * Returns the RPC interface.
     *
     * @return RPC interface.
     */
    Class<?> rpcInterface();

    /**
     * Returns the RPC interface version.
     *
     * @return RPC interface version.
     *
     * @see Rpc#version()
     */
    int rpcVersion();

    /**
     * Returns the RPC interface tag.
     *
     * @return RPC interface tag.
     *
     * @see RpcClientConfig#setTag(String)
     */
    String rpcTag();

    /**
     * Returns the RPC method.
     *
     * @return Method.
     *
     * @see #args()
     */
    Method method();

    /**
     * Returns arguments of this RPC request.
     *
     * @return Arguments or {@code null} if {@link #method() method} doesn't have any arguments.
     *
     * @see #hasArgs()
     * @see #method()
     */
    Object[] args();

    /**
     * Returns {@code true} if RPC request has arguments.
     *
     * @return {@code true} if RPC request has arguments.
     *
     * @see #args()
     */
    boolean hasArgs();

    /**
     * Returns {@code true} if this is an {@link RpcAggregate} request with {@link RpcSplit} of an argument.
     *
     * @return {@code true} if this is an {@link RpcAggregate} request with {@link RpcSplit} of an argument.
     */
    boolean isSplit();
}
