package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.loadbalance.LoadBalancerContext;
import io.hekate.messaging.loadbalance.LoadBalancerException;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.rpc.RpcAggregate;
import io.hekate.rpc.RpcException;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.internal.RpcProtocol.CallRequest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.rpc.internal.RpcUtils.mergeToList;
import static io.hekate.rpc.internal.RpcUtils.mergeToMap;
import static io.hekate.rpc.internal.RpcUtils.mergeToSet;

class RpcSplitAggregateMethodClient<T> extends RpcMethodClientBase<T> {
    private interface Splitter {
        Object[] split(Object arg, int clusterSize);
    }

    private static class RoundRobin implements LoadBalancer<RpcProtocol> {
        private static final AtomicIntegerFieldUpdater<RoundRobin> IDX = AtomicIntegerFieldUpdater.newUpdater(RoundRobin.class, "idx");

        @SuppressWarnings("unused") // <- Updated via AtomicIntegerFieldUpdater.
        private volatile int idx;

        @Override
        public ClusterNodeId route(RpcProtocol message, LoadBalancerContext ctx) throws LoadBalancerException {
            int nodeIdx = IDX.getAndIncrement(this);

            return ctx.topology().nodes().get(Utils.mod(nodeIdx, ctx.size())).id();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RpcService.class);

    private final int splitArgIdx;

    private final Splitter splitter;

    private final BiConsumer<CompletableFuture<Object>, Throwable> errorCheck;

    private final Function<List<RpcProtocol>, Object> aggregator;

    public RpcSplitAggregateMethodClient(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        super(rpc, tag, method, channel);

        assert method.aggregate().isPresent() : "Not an aggregate method [rpc=" + rpc + ", method=" + method + ']';
        assert method.splitArg().isPresent() : "Split argument index is not defined.";

        this.splitArgIdx = method.splitArg().getAsInt();

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Splitting.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        Class<?> splitArgType = method.javaMethod().getParameterTypes()[splitArgIdx];

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
        if (method.realReturnType().equals(Map.class)) {
            // Map.
            aggregator = results -> {
                Map<Object, Object> merged = new HashMap<>();

                results.forEach(result ->
                    mergeToMap(result, merged)
                );

                return merged;
            };
        } else if (method.realReturnType().equals(Set.class)) {
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
        RpcAggregate cfg = method.aggregate().get();

        if (cfg.remoteErrors() == RpcAggregate.RemoteErrors.IGNORE) {
            errorCheck = null;
        } else {
            errorCheck = (future, err) -> {
                if (cfg.remoteErrors() == RpcAggregate.RemoteErrors.WARN) {
                    if (log.isWarnEnabled()) {
                        log.warn("RPC aggregation failed [rpc={}, method={}]", rpc, method, err);
                    }
                } else {
                    String errMsg = "RPC aggregation failed [rpc=" + rpc + ", method=" + method + "]";

                    future.completeExceptionally(new RpcException(errMsg));
                }
            };
        }
    }

    @Override
    protected Object doInvoke(MessagingChannel<RpcProtocol> channel, Object[] args) throws MessagingFutureException,
        InterruptedException, TimeoutException {
        // RPC messaging future.
        MessagingFuture<Object> future = new MessagingFuture<>();

        int clusterSize = channel.cluster().topology().size();

        // Check that RPC topology is not empty.
        if (clusterSize == 0) {
            EmptyTopologyException err = new EmptyTopologyException("No suitable RPC servers [rpc=" + rpc() + ']');

            future.completeExceptionally(err);
        } else {
            // Split argument into parts.
            Object[] parts = split(args, clusterSize);

            // Use Round Robin load balancing to distribute parts among the cluster nodes.
            MessagingChannel<RpcProtocol> roundRobin = channel.withLoadBalancer(new RoundRobin());

            // Process each part as a separate RPC request with a shared callback.
            ResponseCallback<RpcProtocol> partCallback = prepareCallback(future, parts.length);

            for (Object part : parts) {
                // Substitute the original argument with the part that should be submitted to the remote node.
                Object[] partArgs = substituteArgs(args, part);

                // Submit RPC request.
                CallRequest<T> call = new CallRequest<>(rpc(), tag(), method(), partArgs);

                roundRobin.request(call, partCallback);
            }
        }

        // Return results.
        if (method().isAsync()) {
            return future;
        } else {
            if (channel.timeout() > 0) {
                return future.get(channel.timeout(), TimeUnit.MILLISECONDS);
            } else {
                return future.get();
            }
        }
    }

    private Object[] split(Object[] args, int clusterSize) {
        Object arg = ArgAssert.notNull(args[splitArgIdx], "Splittable argument");

        return splitter.split(arg, clusterSize);
    }

    private ResponseCallback<RpcProtocol> prepareCallback(MessagingFuture<Object> future, int parts) {
        List<RpcProtocol> results = new ArrayList<>(parts);

        return (err, rsp) -> {
            if (!future.isDone()) {
                // Check for errors.
                if (err != null && errorCheck != null) {
                    errorCheck.accept(future, err);

                    // Skip processing if future had been already completed by the error check.
                    if (future.isDone()) {
                        return;
                    }
                }

                // Collect results.
                boolean aggregated = false;
                Object aggregate = null;

                synchronized (results) {
                    RpcProtocol result = err == null ? rsp.get() : null;

                    results.add(result);

                    // Check if we've collected all results.
                    if (results.size() == parts) {
                        aggregated = true;

                        aggregate = aggregator.apply(results);
                    }
                }

                // Complete if we've collected all results.
                if (aggregated) {
                    future.complete(aggregate);
                }
            }
        };
    }

    private Object[] substituteArgs(Object[] args, Object part) {
        Object[] partArgs = new Object[args.length];

        System.arraycopy(args, 0, partArgs, 0, args.length);

        partArgs[splitArgIdx] = part;

        return partArgs;
    }

    private Map<Object, Object>[] splitMap(int clusterSize, Map<Object, Object> map) {
        @SuppressWarnings("unchecked")
        Map<Object, Object>[] parts = new Map[clusterSize];

        int partCapacity = map.size() / clusterSize + 1;

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new HashMap<>(partCapacity, 1.0f);
        }

        int idx = 0;

        for (Map.Entry<Object, Object> e : map.entrySet()) {
            if (idx == clusterSize) {
                idx = 0;
            }

            parts[idx++].put(e.getKey(), e.getValue());

        }

        return parts;
    }

    private Collection<Object>[] splitCollection(int clusterSize, Collection<Object> col) {
        @SuppressWarnings("unchecked")
        Collection<Object>[] parts = new Collection[clusterSize];

        int partCapacity = col.size() / clusterSize + 1;

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new ArrayList<>(partCapacity);
        }

        int idx = 0;

        for (Object o : col) {
            if (idx == clusterSize) {
                idx = 0;
            }

            parts[idx++].add(o);

        }

        return parts;
    }

    private Set<Object>[] splitSet(int clusterSize, Set<Object> set) {
        @SuppressWarnings("unchecked")
        Set<Object>[] parts = new Set[clusterSize];

        int partCapacity = set.size() / clusterSize + 1;

        for (int i = 0; i < parts.length; i++) {
            parts[i] = new HashSet<>(partCapacity, 1.0f);
        }

        int idx = 0;

        for (Object o : set) {
            if (idx == clusterSize) {
                idx = 0;
            }

            parts[idx++].add(o);

        }

        return parts;
    }
}
