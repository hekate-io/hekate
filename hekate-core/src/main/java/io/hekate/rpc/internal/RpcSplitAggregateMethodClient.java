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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancers;
import io.hekate.messaging.retry.GenericRetryConfigurer;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.internal.RpcProtocol.RpcCall;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.rpc.internal.RpcUtils.mergeToList;
import static io.hekate.rpc.internal.RpcUtils.mergeToMap;
import static io.hekate.rpc.internal.RpcUtils.mergeToSet;

class RpcSplitAggregateMethodClient<T> extends RpcMethodClientBase<T> {
    private static final Logger log = LoggerFactory.getLogger(RpcService.class);

    private final int splitArgIdx;

    private final RpcArgSplitter splitter;

    private final RpcErrorMappingPolicy errorPolicy;

    private final Function<List<RpcProtocol>, Object> aggregator;

    private final GenericRetryConfigurer retryPolicy;

    private final long timeout;

    public RpcSplitAggregateMethodClient(
        RpcInterfaceInfo<T> rpc,
        String tag,
        RpcMethodInfo method,
        MessagingChannel<RpcProtocol> channel,
        GenericRetryConfigurer retryPolicy,
        long timeout
    ) {
        super(rpc, tag, method, channel);

        assert method.aggregate().isPresent() : "Not an aggregate method [rpc=" + rpc + ", method=" + method + ']';
        assert method.splitArg().isPresent() : "Split argument index is not defined.";
        assert method.splitArgType().isPresent() : "Split argument index is not defined.";

        this.timeout = timeout;
        this.retryPolicy = retryPolicy;
        this.splitArgIdx = method.splitArg().getAsInt();

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Splitting.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        Class<?> splitArgType = method.splitArgType().get();

        if (splitArgType.equals(Map.class)) {
            // Map.
            splitter = (arg, clusterSize) -> {
                @SuppressWarnings("unchecked")
                Map<Object, Object> map = (Map<Object, Object>)arg;

                return splitMap(clusterSize, map);
            };
        } else if (splitArgType.equals(Set.class)) {
            // Set.
            splitter = (arg, clusterSize) -> {
                @SuppressWarnings("unchecked")
                Set<Object> set = (Set<Object>)arg;

                return splitSet(clusterSize, set);
            };
        } else {
            // Collection/List.
            splitter = (arg, clusterSize) -> {
                @SuppressWarnings("unchecked")
                Collection<Object> col = (Collection<Object>)arg;

                return splitCollection(clusterSize, col);
            };
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Aggregation.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        Class<?> returnType = method.realReturnType();

        if (returnType.equals(Map.class)) {
            // Map.
            aggregator = results -> {
                Map<Object, Object> merged = new HashMap<>();

                results.forEach(result ->
                    mergeToMap(result, merged)
                );

                return merged;
            };
        } else if (returnType.equals(Set.class)) {
            // Set.
            aggregator = results -> {
                Set<Object> merged = new HashSet<>();

                results.forEach(result ->
                    mergeToSet(result, merged)
                );

                return merged;
            };
        } else {
            // Collection/List.
            aggregator = results -> {
                List<Object> merged = new ArrayList<>();

                results.forEach(result ->
                    mergeToList(result, merged)
                );

                return merged;
            };
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Error handling.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        RpcAggregate.RemoteErrors policy = method.aggregate().get().remoteErrors();

        if (policy == RpcAggregate.RemoteErrors.IGNORE) {
            errorPolicy = err -> {
                if (log.isDebugEnabled()) {
                    log.debug("RPC aggregation failed [method={}]", method, err);
                }

                return null;
            };
        } else {
            errorPolicy = err -> {
                if (policy == RpcAggregate.RemoteErrors.WARN) {
                    if (log.isWarnEnabled()) {
                        log.warn("RPC aggregation failed [method={}#{}]", rpc.name(), method.signature(), err);
                    }

                    return null;
                } else {
                    String errMsg = "RPC aggregation failed [method=" + rpc.name() + '#' + method.signature() + ']';

                    return new RpcException(errMsg, err);
                }
            };
        }
    }

    @Override
    protected Object doInvoke(Object affinity, Object[] args) throws MessagingFutureException, InterruptedException, TimeoutException {
        // RPC messaging future.
        MessagingFuture<Object> future;

        int clusterSize = channel().cluster().topology().size();

        // Check that RPC topology is not empty.
        if (clusterSize == 0) {
            EmptyTopologyException err = new EmptyTopologyException("No suitable RPC servers [rpc=" + rpc() + ']');

            future = new MessagingFuture<>();

            future.completeExceptionally(err);
        } else {
            // Split argument into parts.
            Object[] parts = split(args, clusterSize);

            RpcSplitAggregateFuture aggrFuture = new RpcSplitAggregateFuture(parts.length, errorPolicy, aggregator);

            // Use Round Robin load balancing to distribute parts among the cluster nodes.
            MessagingChannel<RpcProtocol> roundRobin = channel().withLoadBalancer(LoadBalancers.newRoundRobin());

            // Process each part as a separate RPC request with a shared callback.
            for (Object part : parts) {
                // Replace the original argument with the part that should be sent to the remote node.
                Object[] partArgs = substituteArgs(args, part);

                // Submit RPC request.
                RpcCall<T> call = new RpcCall<>(methodIdxKey(), rpc(), tag(), method(), partArgs, true /* <- Split. */);

                roundRobin.newRequest(call)
                    .withTimeout(timeout, TimeUnit.MILLISECONDS)
                    .withRetry(retry -> {
                        if (retryPolicy != null) {
                            retryPolicy.configure(retry);
                        }
                    })
                    .submit(aggrFuture); // <- Future is a callback.
            }

            future = aggrFuture;
        }

        // Return results.
        if (method().isAsync()) {
            return future;
        } else {
            return future.get();
        }
    }

    private Object[] split(Object[] args, int clusterSize) {
        Object arg = ArgAssert.notNull(args[splitArgIdx], "Splittable argument");

        return splitter.split(arg, clusterSize);
    }

    private Object[] substituteArgs(Object[] args, Object part) {
        Object[] partArgs = new Object[args.length];

        System.arraycopy(args, 0, partArgs, 0, args.length);

        partArgs[splitArgIdx] = part;

        return partArgs;
    }

    private static Map<Object, Object>[] splitMap(int clusterSize, Map<Object, Object> map) {
        @SuppressWarnings("unchecked")
        Map<Object, Object>[] parts = new Map[Math.min(clusterSize, map.size())];

        int partCapacity = partCapacity(map.size(), parts.length);

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new HashMap<>(partCapacity, 1.0f);
        }

        int idx = 0;

        for (Map.Entry<Object, Object> e : map.entrySet()) {
            if (idx == parts.length) {
                idx = 0;
            }

            parts[idx++].put(e.getKey(), e.getValue());
        }

        return parts;
    }

    private static Collection<Object>[] splitCollection(int clusterSize, Collection<Object> col) {
        @SuppressWarnings("unchecked")
        Collection<Object>[] parts = new Collection[Math.min(clusterSize, col.size())];

        int partCapacity = partCapacity(col.size(), parts.length);

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new ArrayList<>(partCapacity);
        }

        int idx = 0;

        for (Object o : col) {
            if (idx == parts.length) {
                idx = 0;
            }

            parts[idx++].add(o);
        }

        return parts;
    }

    private static Set<Object>[] splitSet(int clusterSize, Set<Object> set) {
        @SuppressWarnings("unchecked")
        Set<Object>[] parts = new Set[Math.min(clusterSize, set.size())];

        int partCapacity = partCapacity(set.size(), parts.length);

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new HashSet<>(partCapacity, 1.0f);
        }

        int idx = 0;

        for (Object o : set) {
            if (idx == parts.length) {
                idx = 0;
            }

            parts[idx++].add(o);
        }

        return parts;
    }

    private static int partCapacity(int size, int clusterSize) {
        return size / clusterSize + 1;
    }
}
