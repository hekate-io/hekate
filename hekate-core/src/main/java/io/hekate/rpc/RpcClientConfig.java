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

import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.Partition;
import io.hekate.partition.RendezvousHashMapper;
import io.hekate.util.format.ToString;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * RPC client configuration.
 *
 * <p>
 * This class provides support for pre-configuring some of the {@link RpcClientBuilder}'s options. When {@link RpcClientBuilder} is
 * requested from the {@link RpcService} it first searches for an existing configuration by using the following rules:
 * </p>
 *
 * <ul>
 * <li>if {@link RpcService#clientFor(Class)} method is used then it will search for an instance of {@link RpcClientConfig} that has the
 * same {@link #setRpcInterface(Class) RPC interface} and a {@code null} {@link #setTag(String) tag}</li>
 * <li>if {@link RpcService#clientFor(Class, String)} method is used then it will search for an instance of {@link RpcClientConfig} that
 * has the same {@link #setRpcInterface(Class) RPC interface} and the same {@link #setTag(String) tag} value</li>
 * </ul>
 *
 * <p>
 * If such {@link RpcClientConfig} instance is found then its options will be applied to the {@link RpcClientBuilder}.
 * </p>
 *
 * <p>
 * For more details about the Remote Procedure Call API and its capabilities please see the documentation of the {@link RpcService}
 * interface.
 * </p>
 *
 * @see RpcService#clientFor(Class)
 * @see RpcService#clientFor(Class, String)
 */
public class RpcClientConfig {
    private Class<?> rpcInterface;

    private String tag;

    private RpcLoadBalancer loadBalancer;

    private int partitions = RendezvousHashMapper.DEFAULT_PARTITIONS;

    private int backupNodes;

    private long timeout;

    private GenericRetryConfigurer retryPolicy;

    /**
     * Returns the RPC interface (see {@link #setRpcInterface(Class)}).
     *
     * @return RPC interface.
     */
    public Class<?> getRpcInterface() {
        return rpcInterface;
    }

    /**
     * Sets the RPC interface.
     *
     * <p>
     * The specified interface must be public and must be annotated with {@link Rpc}.
     * </p>
     *
     * @param rpcInterface RPC interface.
     */
    public void setRpcInterface(Class<?> rpcInterface) {
        this.rpcInterface = rpcInterface;
    }

    /**
     * Fluent-style version of {@link #setRpcInterface(Class)}.
     *
     * @param rpcInterface RPC interface.
     *
     * @return This instance.
     */
    public RpcClientConfig withRpcInterface(Class<?> rpcInterface) {
        setRpcInterface(rpcInterface);

        return this;
    }

    /**
     * Returns the RPC interface tag (see {@link #setTag(String)}).
     *
     * @return RPC interface tag.
     */
    public String getTag() {
        return tag;
    }

    /**
     * Sets the RPC interface tag.
     *
     * <p>
     * If this parameter is specified then RPC requests will be submitted only to those servers that have the same tag set via {@link
     * RpcServerConfig#setTags(Set)}. If this parameter is not specified then client will be able to submit RPC requests only to
     * those servers that don't have any tags too.
     * </p>
     *
     * @param tag RPC interface tag.
     *
     * @see RpcServerConfig#setTags(Set)
     * @see RpcService#clientFor(Class, String)
     */
    public void setTag(String tag) {
        this.tag = tag;
    }

    /**
     * Fluent-style version of {@link #setTag(String)}.
     *
     * @param tag RPC interface tag.
     *
     * @return This instance.
     */
    public RpcClientConfig withTag(String tag) {
        setTag(tag);

        return this;
    }

    /**
     * Returns the RPC load balancer (see {@link #setLoadBalancer(RpcLoadBalancer)}).
     *
     * @return RPC load balancer.
     */
    public RpcLoadBalancer getLoadBalancer() {
        return loadBalancer;
    }

    /**
     * Sets the RPC load balancer.
     *
     * @param loadBalancer RPC load balancer.
     */
    public void setLoadBalancer(RpcLoadBalancer loadBalancer) {
        this.loadBalancer = loadBalancer;
    }

    /**
     * Fluent-style version of {@link #setLoadBalancer(RpcLoadBalancer)}.
     *
     * @param loadBalancer RPC load balancer.
     *
     * @return This instance.
     *
     * @see RpcClientBuilder#withLoadBalancer(RpcLoadBalancer)
     */
    public RpcClientConfig withLoadBalancer(RpcLoadBalancer loadBalancer) {
        setLoadBalancer(loadBalancer);

        return this;
    }

    /**
     * Returns the total amount of partitions that should be managed by the RPC client's {@link RpcClientBuilder#partitions() partition
     * mapper} (see {@link #setPartitions(int)}).
     *
     * @return Total amount of partitions.
     */
    public int getPartitions() {
        return partitions;
    }

    /**
     * Sets the total amount of partitions that should be managed by the RPC client's
     * {@link RpcClientBuilder#partitions() partition mapper}.
     *
     * <p>
     * Value of this parameter must be above zero and must be a power of two.
     * Default value is specified by {@link RendezvousHashMapper#DEFAULT_PARTITIONS}.
     * </p>
     *
     * @param partitions Total amount of partitions that should be managed by the RPC client's partition mapper (value must be a power of
     * two).
     */
    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    /**
     * Fluent-style version of {@link #setPartitions(int)}.
     *
     * @param partitions Total amount of partitions that should be managed by the RPC client's partition mapper (value must be a power of
     * two).
     *
     * @return This instance.
     */
    public RpcClientConfig withPartitions(int partitions) {
        setPartitions(partitions);

        return this;
    }

    /**
     * Returns the amount of backup nodes that should be assigned to each partition by the the RPC client's
     * {@link RpcClientBuilder#partitions() partition mapper} (see {@link #setBackupNodes(int)}).
     *
     * @return Amount of backup nodes.
     */
    public int getBackupNodes() {
        return backupNodes;
    }

    /**
     * Sets the amount of backup nodes that should be assigned to each partition by the the RPC client's
     * {@link RpcClientBuilder#partitions() partition mapper}.
     *
     * <p>
     * If value of this parameter is zero then the RPC client's mapper will not manage {@link Partition#backupNodes() backup nodes}.
     * If value of this parameter is negative then all available cluster nodes will be used as {@link Partition#backupNodes() backup nodes}.
     * </p>
     *
     * <p>
     * Default value of this parameter is 0 (i.e. backup nodes management is disabled).
     * </p>
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition of the RPC client's partition mapper.
     */
    public void setBackupNodes(int backupNodes) {
        this.backupNodes = backupNodes;
    }

    /**
     * Fluent-style version of {@link #setBackupNodes(int)}.
     *
     * @param backupNodes Amount of backup nodes that should be assigned to each partition of the RPC client's partition mapper.
     *
     * @return This instance.
     */
    public RpcClientConfig withBackupNodes(int backupNodes) {
        setBackupNodes(backupNodes);

        return this;
    }

    /**
     * Returns the timeout in milliseconds (see {@link #setTimeout(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the RPC timeout in milliseconds.
     *
     * <p>
     * If the RPC operation can not be completed at the specified timeout then such operation will end up an error.
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * @param timeout RPC timeout in milliseconds.
     *
     * @see RpcClientBuilder#withTimeout(long, TimeUnit)
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Fluent-style version of {@link #setTimeout(long)}.
     *
     * @param timeout RPC timeout in milliseconds.
     *
     * @return This instance.
     */
    public RpcClientConfig withTimeout(long timeout) {
        setTimeout(timeout);

        return this;
    }

    /**
     * Returns the generic retry policy (see {@link #setRetryPolicy(GenericRetryConfigurer)}).
     *
     * @return Generic retry policy.
     */
    public GenericRetryConfigurer getRetryPolicy() {
        return retryPolicy;
    }

    /**
     * Sets the generic retry policy.
     *
     * <p>
     * <b>Notice:</b>
     * The specified retry policy will be applied only to those RPC methods that are explicitly marked with the {@link RpcRetry} annotation.
     * Options that are defined in the annotation's attributes have higher priority over this policy.
     * </p>
     *
     * <p>
     * Alternatively the generic retry policy can be specified at the RPC client construction time via the
     * {@link RpcClientBuilder#withRetryPolicy(GenericRetryConfigurer)} method.
     * </p>
     *
     * @param retryPolicy Generic retry policy.
     *
     * @see RpcRetry
     */
    public void setRetryPolicy(GenericRetryConfigurer retryPolicy) {
        this.retryPolicy = retryPolicy;
    }

    /**
     * Fluent-style version of {@link #setRetryPolicy(GenericRetryConfigurer)}.
     *
     * @param retryPolicy Generic retry policy.
     *
     * @return This instance.
     */
    public RpcClientConfig withRetryPolicy(GenericRetryConfigurer retryPolicy) {
        setRetryPolicy(retryPolicy);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
