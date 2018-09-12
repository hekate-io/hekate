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
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.ResponseFuture;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.internal.RpcProtocol.RpcCall;
import io.hekate.rpc.internal.RpcProtocol.RpcCallResult;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class RpcMethodClient<T> extends RpcMethodClientBase<T> {
    private static final Function<Response<RpcProtocol>, Object> RESPONSE_CONVERTER = response -> {
        RpcProtocol result = response.get();

        if (result instanceof RpcCallResult) {
            return ((RpcCallResult)result).result();
        } else {
            return null;
        }
    };

    public RpcMethodClient(RpcInterfaceInfo<T> rpc, String tag, RpcMethodInfo method, MessagingChannel<RpcProtocol> channel) {
        super(rpc, tag, method, channel);
    }

    @Override
    protected Object doInvoke(MessagingChannel<RpcProtocol> callChannel, Object[] args) throws MessagingFutureException,
        InterruptedException, TimeoutException {
        RpcCall<T> request = new RpcCall<>(methodIdxKey(), rpc(), tag(), method(), args);

        ResponseFuture<RpcProtocol> future = callChannel.request(request);

        if (method().isAsync()) {
            return future.thenApply(RESPONSE_CONVERTER);
        } else {
            Response<RpcProtocol> response;

            if (callChannel.timeout() > 0) {
                response = future.get(callChannel.timeout(), TimeUnit.MILLISECONDS);
            } else {
                response = future.get();
            }

            return RESPONSE_CONVERTER.apply(response);
        }
    }
}
