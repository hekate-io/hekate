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

package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterView;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.partition.PartitionMapper;
import io.hekate.rpc.RpcClientBuilder;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcLoadBalancer;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcRequest;
import io.hekate.rpc.RpcRetryInfo;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

class DefaultRpcClientBuilder<T> implements RpcClientBuilder<T> {
    private final RpcInterfaceInfo<T> type;

    private final String tag;

    private final long timeout;

    private final GenericRetryConfigurer retryPolicy;

    @ToStringIgnore
    private final MessagingChannel<RpcProtocol> channel;

    public DefaultRpcClientBuilder(
        RpcInterfaceInfo<T> type,
        String tag,
        MessagingChannel<RpcProtocol> channel,
        long timeout,
        GenericRetryConfigurer retryPolicy
    ) {
        assert type != null : "RPC type is null.";
        assert channel != null : "Messaging channel is null.";

        this.type = type;
        this.tag = tag;
        this.channel = channel;
        this.timeout = timeout;
        this.retryPolicy = retryPolicy;
    }

    @Override
    public RpcClientBuilder<T> withLoadBalancer(RpcLoadBalancer balancer) {
        LoadBalancer<RpcProtocol> rpcBalancer = (message, ctx) -> {
            RpcRequest request = (RpcRequest)message;

            return balancer.route(request, ctx);
        };

        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel.withLoadBalancer(rpcBalancer),
            timeout,
            retryPolicy
        );
    }

    @Override
    public RpcClientBuilder<T> withRetryPolicy(GenericRetryConfigurer retry) {
        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel,
            timeout,
            retry
        );
    }

    @Override
    public long timeout() {
        return timeout;
    }

    @Override
    public RpcClientBuilder<T> withTimeout(long timeout, TimeUnit unit) {
        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel,
            unit.toMillis(timeout),
            retryPolicy
        );
    }

    @Override
    public RpcClientBuilder<T> filterAll(ClusterFilter filter) {
        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel.filterAll(filter),
            timeout,
            retryPolicy
        );
    }

    @Override
    public PartitionMapper partitions() {
        return channel.partitions();
    }

    @Override
    public RpcClientBuilder<T> withPartitions(int partitions, int backupNodes) {
        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel.withPartitions(partitions, backupNodes),
            timeout,
            retryPolicy
        );
    }

    @Override
    public Class<T> type() {
        return type.javaType();
    }

    @Override
    public String tag() {
        return tag;
    }

    @Override
    public ClusterView cluster() {
        return channel.cluster();
    }

    @Override
    public RpcClientBuilder<T> withCluster(ClusterView cluster) {
        return new DefaultRpcClientBuilder<>(
            type,
            tag,
            channel.withCluster(cluster.filter(RpcUtils.filterFor(type, tag))),
            timeout,
            retryPolicy
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public T build() {
        ClassLoader classLoader = type.javaType().getClassLoader();

        Map<Method, RpcMethodClientBase<T>> clients = new HashMap<>(type.methods().size(), 1.0f);

        for (RpcMethodInfo method : type.methods()) {
            RpcMethodClientBase<T> client;

            if (method.splitArg().isPresent()) {
                client = new RpcSplitAggregateMethodClient<>(type, tag, method, channel, retryPolicy(method), timeout);
            } else if (method.aggregate().isPresent()) {
                client = new RpcAggregateMethodClient<>(type, tag, method, channel, retryPolicy(method), timeout);
            } else if (method.broadcast().isPresent()) {
                client = new RpcBroadcastMethodClient<>(type, tag, method, channel, retryPolicy(method), timeout);
            } else {
                client = new RpcMethodClient<>(type, tag, method, channel, retryPolicy(method), timeout);
            }

            clients.put(method.javaMethod(), client);
        }

        Class<?>[] proxyType = {type.javaType()};

        return (T)Proxy.newProxyInstance(classLoader, proxyType, (proxy, method, args) -> {
            RpcMethodClientBase client = clients.get(method);

            if (client == null) {
                if (method.getDeclaringClass().equals(Object.class)) {
                    return method.invoke(this, args);
                } else {
                    throw new UnsupportedOperationException("Method is not supported by RPC: " + method);
                }
            } else {
                return client.invoke(args);
            }
        });
    }

    private GenericRetryConfigurer retryPolicy(RpcMethodInfo method) {
        if (method.retry().isPresent()) {
            RpcRetryInfo methodRetryPolicy = method.retry().get();

            return retry -> {
                // Apply global policy first.
                if (retryPolicy != null) {
                    retryPolicy.configure(retry);
                }

                // Apply the method retry policy.
                methodRetryPolicy.configure(retry);
            };
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
