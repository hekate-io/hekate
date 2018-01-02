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
