package io.hekate.rpc.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.EmptyTopologyException;
import io.hekate.messaging.loadbalance.LoadBalancers;
import io.hekate.messaging.unicast.Response;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

    private static class AggregateFuture extends MessagingFuture<Object> implements ResponseCallback<RpcProtocol> {
        private final int parts;

        private final List<RpcProtocol> responses;

        private final Function<Throwable, Throwable> errorPolicy;

        private final Function<List<RpcProtocol>, Object> aggregator;

        private int collectedParts;

        private Throwable firstError;

        public AggregateFuture(int parts, Function<Throwable, Throwable> errorPolicy, Function<List<RpcProtocol>, Object> aggregator) {
            this.parts = parts;
            this.errorPolicy = errorPolicy;
            this.aggregator = aggregator;
            this.responses = new ArrayList<>(parts);
        }

        @Override
        public void onComplete(Throwable err, Response<RpcProtocol> rsp) {
            if (!isDone()) {
                Throwable mappedErr = null;

                // Check for errors.
                if (err != null && errorPolicy != null) {
                    mappedErr = errorPolicy.apply(err);
                }

                // Collect/aggregate results.
                boolean allDone = false;

                Object okResult = null;
                Throwable errResult = null;

                synchronized (responses) {
                    if (mappedErr == null) {
                        if (rsp != null) { // <-- Can be null if there was a real error but it was ignored by the error policy.
                            responses.add(rsp.get());
                        }
                    } else {
                        if (firstError == null) {
                            firstError = mappedErr;
                        }
                    }

                    // Check if we've collected all results.
                    collectedParts++;

                    if (collectedParts == parts) {
                        allDone = true;

                        // Check if should complete successfully or exceptionally.
                        if (firstError == null) {
                            okResult = aggregator.apply(responses);
                        } else {
                            errResult = firstError;
                        }
                    }
                }

                // Complete if we've collected all results.
                if (allDone) {
                    if (errResult == null) {
                        complete(okResult);
                    } else {
                        completeExceptionally(errResult);
                    }
                }
            }
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RpcService.class);

    private final int splitArgIdx;

    private final Splitter splitter;

    private final Function<Throwable, Throwable> errorPolicy;

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
            errorPolicy = null;
        } else {
            errorPolicy = err -> {
                if (cfg.remoteErrors() == RpcAggregate.RemoteErrors.WARN) {
                    if (log.isWarnEnabled()) {
                        log.warn("RPC aggregation failed [rpc={}, method={}]", rpc, method, err);
                    }

                    return null;
                } else if (cfg.remoteErrors() == RpcAggregate.RemoteErrors.IGNORE) {
                    if (log.isDebugEnabled()) {
                        log.debug("RPC aggregation failed [rpc={}, method={}]", rpc, method, err);
                    }

                    return null;
                } else {
                    String errMsg = "RPC aggregation failed [rpc=" + rpc + ", method=" + method + "]";

                    return new RpcException(errMsg, err);
                }
            };
        }
    }

    @Override
    protected Object doInvoke(MessagingChannel<RpcProtocol> channel, Object[] args) throws MessagingFutureException,
        InterruptedException, TimeoutException {
        // RPC messaging future.
        MessagingFuture<Object> future;

        int clusterSize = channel.cluster().topology().size();

        // Check that RPC topology is not empty.
        if (clusterSize == 0) {
            EmptyTopologyException err = new EmptyTopologyException("No suitable RPC servers [rpc=" + rpc() + ']');

            future = new MessagingFuture<>();

            future.completeExceptionally(err);
        } else {
            // Split argument into parts.
            Object[] parts = split(args, clusterSize);

            AggregateFuture aggregateFuture = new AggregateFuture(parts.length, errorPolicy, aggregator);

            // Use Round Robin load balancing to distribute parts among the cluster nodes.
            MessagingChannel<RpcProtocol> roundRobin = channel.withLoadBalancer(LoadBalancers.roundRobin());

            // Process each part as a separate RPC request with a shared callback.
            for (Object part : parts) {
                // Substitute the original argument with the part that should be submitted to the remote node.
                Object[] partArgs = substituteArgs(args, part);

                // Submit RPC request.
                CallRequest<T> call = new CallRequest<>(rpc(), tag(), method(), partArgs);

                roundRobin.request(call, aggregateFuture /* <-- Future is a callback. */);
            }

            future = aggregateFuture;
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

    private Object[] substituteArgs(Object[] args, Object part) {
        Object[] partArgs = new Object[args.length];

        System.arraycopy(args, 0, partArgs, 0, args.length);

        partArgs[splitArgIdx] = part;

        return partArgs;
    }

    private Map<Object, Object>[] splitMap(int clusterSize, Map<Object, Object> map) {
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

    private Collection<Object>[] splitCollection(int clusterSize, Collection<Object> col) {
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

    private Set<Object>[] splitSet(int clusterSize, Set<Object> set) {
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

    private int partCapacity(int size, int clusterSize) {
        return size / clusterSize + 1;
    }
}
