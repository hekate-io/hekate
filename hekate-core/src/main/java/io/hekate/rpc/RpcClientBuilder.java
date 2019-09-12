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

import io.hekate.cluster.ClusterFilterSupport;
import io.hekate.cluster.ClusterView;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.PartitionMapper;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Builder for RPC client proxies.
 *
 * <p>
 * For more details about the Remote Procedure Call API and its capabilities please see the documentation of the {@link RpcService}
 * interface.
 * </p>
 *
 * @param <T> RPC interface type.
 *
 * @see RpcService#clientFor(Class)
 * @see RpcService#clientFor(Class, String)
 */
public interface RpcClientBuilder<T> extends ClusterFilterSupport<RpcClientBuilder<T>> {
    /**
     * Constructs a new RPC client proxy.
     *
     * <p>
     * <b>Note:</b> this operation is relatively expensive and it is highly recommended to cache RPC proxy instances that are produced by
     * this method.
     * </p>
     *
     * @return new RPC client proxy instance.
     */
    T build();

    /**
     * Returns the RPC interface type.
     *
     * @return RPC interface type.
     *
     * @see Rpc
     */
    Class<T> type();

    /**
     * Returns the RPC tag value.
     *
     * @return RPC tag value.
     *
     * @see RpcService#clientFor(Class, String)
     * @see RpcServerConfig#setTags(Set)
     */
    String tag();

    /**
     * Returns a new builder that will apply the specifier load balancer to all clients that it produces.
     *
     * <p>
     * Alternatively, the load balancer can be pre-configured via {@link RpcClientConfig#setLoadBalancer(RpcLoadBalancer)} method.
     * </p>
     *
     * @param balancer Load balancer.
     *
     * @return New builder that will use the specified load balancer and will inherit all other options from this builder.
     */
    RpcClientBuilder<T> withLoadBalancer(RpcLoadBalancer balancer);

    /**
     * Returns a new builder that will apply the specifier retry policy to all clients that it produces.
     *
     * <p>
     * <b>Notice:</b>
     * The specified retry policy will be applied only to those RPC methods that are explicitly marked with the {@link RpcRetry} annotation.
     * Options that are defined in the annotation's attributes have higher priority over this policy.
     * </p>
     *
     * @param retry Retry policy.
     *
     * @return New builder that will use the specified retry policy and will inherit all other options from this builder.
     */
    RpcClientBuilder<T> withRetryPolicy(GenericRetryConfigurer retry);

    /**
     * Returns the timeout value in milliseconds (see {@link #withTimeout(long, TimeUnit)}).
     *
     * @return Timeout in milliseconds or 0, if timeout was not specified.
     */
    long timeout();

    /**
     * Returns a new builder that will apply the specifier timeout to all clients that it produces.
     *
     * <p>
     * If the RPC operation can not be completed at the specified timeout then such operation will end up an error.
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * <p>
     * Alternatively, the timeout value can be pre-configured via {@link RpcClientConfig#setTimeout(long)} method.
     * </p>
     *
     * @param timeout Timeout.
     * @param unit Time unit.
     *
     * @return New builder that will use the specified timeout and will inherit all other options from this builder.
     */
    RpcClientBuilder<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Returns the cluster view of this builder.
     *
     * <p>
     * All clients that are produced by this builder will use the same cluster view for RPC requests routing and load balancing.
     * </p>
     *
     * @return Cluster view of this builder.
     */
    ClusterView cluster();

    /**
     * Returns a new builder that will use the specified cluster view with all clients that it produces.
     *
     * @param cluster Cluster view.
     *
     * @return Channel wrapper.
     */
    RpcClientBuilder<T> withCluster(ClusterView cluster);

    /**
     * Returns the partition mapper that RPC client will use to map {@link RpcAffinityKey affinity keys} to the cluster nodes.
     *
     * @return Mapper.
     *
     * @see RpcClientConfig#setPartitions(int)
     * @see RpcClientConfig#setBackupNodes(int)
     * @see #withPartitions(int, int)
     */
    PartitionMapper partitions();

    /**
     * Returns a new builder that will apply the specified partitions mapping options to all clients that it produces.
     *
     * @param partitions Total amount of partitions that should be managed by the RPC client's partition mapper.
     * @param backupNodes Amount of backup nodes that should be assigned to each partition by the the RPC client's partition mapper.
     *
     * @return New builder that will apply the specified partitions mapping options to all of the clients that it will produce..
     *
     * @see #partitions()
     */
    RpcClientBuilder<T> withPartitions(int partitions, int backupNodes);
}
