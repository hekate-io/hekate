/*
 * Copyright 2018 The Hekate Project
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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.rpc.RpcAggregateException;
import io.hekate.rpc.RpcBroadcast;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.internal.RpcProtocol.RpcCall;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.emptyMap;

class RpcBroadcastMethodClient<T> extends RpcMethodClientBase<T> {
    private static final Logger log = LoggerFactory.getLogger(RpcService.class);

    private final Function<AggregateResult<RpcProtocol>, ?> converter;

    public RpcBroadcastMethodClient(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        super(rpc, tag, method, channel);

        assert method.broadcast().isPresent() : "Not a broadcast method [rpc=" + rpc + ", method=" + method + ']';

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Error handling.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        RpcBroadcast config = method.broadcast().get();

        Consumer<AggregateResult<RpcProtocol>> errorCheck;

        if (config.remoteErrors() == RpcBroadcast.RemoteErrors.IGNORE) {
            errorCheck = null;
        } else {
            errorCheck = aggregate -> {
                if (!aggregate.isSuccess()) {
                    if (config.remoteErrors() == RpcBroadcast.RemoteErrors.WARN) {
                        if (log.isWarnEnabled()) {
                            aggregate.errors().forEach((node, err) ->
                                log.warn("RPC broadcast failed [remote-node={}, method={}#{}]", node, rpc.name(), method.signature(), err)
                            );
                        }
                    } else {
                        String errMsg = "RPC broadcast failed [method=" + rpc.name() + '#' + method.signature() + ']';

                        throw new RpcAggregateException(errMsg, aggregate.errors(), emptyMap());
                    }
                }
            };
        }

        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // Aggregation of results.
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        converter = aggregate -> {
            if (errorCheck != null) {
                errorCheck.accept(aggregate);
            }

            return null;
        };
    }

    @Override
    protected Object doInvoke(MessagingChannel<RpcProtocol> channel, Object[] args) throws MessagingFutureException,
        InterruptedException, TimeoutException {
        RpcCall<T> request = new RpcCall<>(methodIdxKey(), rpc(), tag(), method(), args);

        AggregateFuture<RpcProtocol> future = channel.aggregate(request);

        if (method().isAsync()) {
            return future.thenApply(converter);
        } else {
            AggregateResult<RpcProtocol> result;

            if (channel.timeout() > 0) {
                result = future.get(channel.timeout(), TimeUnit.MILLISECONDS);
            } else {
                result = future.get();
            }

            return converter.apply(result);
        }
    }
}
