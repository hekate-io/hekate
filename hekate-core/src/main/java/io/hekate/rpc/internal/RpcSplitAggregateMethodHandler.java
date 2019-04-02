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

import io.hekate.messaging.MessagingEndpoint;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RpcSplitAggregateMethodHandler extends RpcMethodHandler {
    private static final Logger log = LoggerFactory.getLogger(RpcSplitAggregateMethodHandler.class);

    private final int splitArgIdx;

    private final RpcArgSplitter splitter;

    private final Function<List<Object>, Object> aggregator;

    public RpcSplitAggregateMethodHandler(RpcInterfaceInfo<?> rpc, RpcMethodInfo method, Object target) {
        super(rpc, method, target);

        assert method.aggregate().isPresent() : "Not an aggregate method [method=" + method + ']';
        assert method.splitArg().isPresent() : "Split argument index is not defined.";
        assert method.splitArgType().isPresent() : "Split argument index is not defined.";

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

                @SuppressWarnings("unchecked")
                Map<Object, Object>[] parts = (Map<Object, Object>[])new Map[map.size()];

                int idx = 0;

                for (Map.Entry<Object, Object> e : map.entrySet()) {
                    parts[idx++] = Collections.singletonMap(e.getKey(), e.getValue());
                }

                return parts;
            };
        } else if (splitArgType.equals(Set.class)) {
            // Set.
            splitter = (arg, clusterSize) -> {
                @SuppressWarnings("unchecked")
                Set<Object> set = (Set<Object>)arg;

                @SuppressWarnings("unchecked")
                Set<Object>[] parts = (Set<Object>[])new Set[set.size()];

                int idx = 0;

                for (Object o : set) {
                    parts[idx++] = Collections.singleton(o);
                }

                return parts;
            };
        } else {
            // Collection/List.
            splitter = (arg, clusterSize) -> {
                @SuppressWarnings("unchecked")
                Collection<Object> col = (Collection<Object>)arg;

                @SuppressWarnings("unchecked")
                List<Object>[] parts = (List<Object>[])new List[col.size()];

                int idx = 0;

                for (Object o : col) {
                    parts[idx++] = Collections.singletonList(o);
                }

                return parts;
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

                results.forEach(result -> {
                    @SuppressWarnings("unchecked")
                    Map<Object, Object> part = (Map<Object, Object>)result;

                    merged.putAll(part);
                });

                return merged;
            };
        } else if (returnType.equals(Set.class)) {
            // Set.
            aggregator = results -> {
                Set<Object> merged = new HashSet<>();

                results.forEach(result -> {
                    @SuppressWarnings("unchecked")
                    Set<Object> part = (Set<Object>)result;

                    merged.addAll(part);
                });

                return merged;
            };
        } else {
            // Collection/List.
            aggregator = results -> {
                List<Object> merged = new ArrayList<>();

                results.forEach(result -> {
                    @SuppressWarnings("unchecked")
                    Collection<Object> part = (Collection<Object>)result;

                    merged.addAll(part);
                });

                return merged;
            };
        }
    }

    @Override
    protected void doHandle(Object[] args, MessagingEndpoint<RpcProtocol> from, BiConsumer<Throwable, Object> callback) {
        int threads = from.channel().workerThreads();

        if (method().isAsync() || threads <= 1) {
            // Handle on the same thread.
            super.doHandle(args, from, callback);
        } else {
            // Split and process in parallel on multiple threads.
            Object[] parts = splitter.split(args[splitArgIdx], threads);

            RpcSplitAggregateCallback aggregateCallback = new RpcSplitAggregateCallback(parts.length, aggregator, callback);

            // Process each part on a separate thread.
            for (Object part : parts) {
                // Substitute the original argument with a part that should be submitted for parallel processing.
                Object[] partArgs = substituteArgs(args, part);

                from.channel().executor().execute(() -> {
                    try {
                        super.doHandle(partArgs, from, aggregateCallback);
                    } catch (RuntimeException | Error e) {
                        if (log.isErrorEnabled()) {
                            log.error("RPC failure [from={}, method={}#{}]", from.remoteAddress(), rpc().name(), method().signature(), e);
                        }
                    }
                });
            }
        }
    }

    private Object[] substituteArgs(Object[] args, Object part) {
        Object[] partArgs = new Object[args.length];

        System.arraycopy(args, 0, partArgs, 0, args.length);

        partArgs[splitArgIdx] = part;

        return partArgs;
    }
}
