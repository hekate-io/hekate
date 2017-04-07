/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.Response;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;

class InMemoryReceiverContext<T> extends ReceiverContext<T> {
    public InMemoryReceiverContext(MessagingGateway<T> gateway, AffinityExecutor async) {
        super(gateway, async, new DefaultMessagingEndpoint<>(gateway.getId(), gateway.getChannel()), false);
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return null;
    }

    @Override
    public void sendNotification(MessageContext<T> ctx, SendCallback callback) {
        Notification<T> msg;

        if (ctx.isStrictAffinity()) {
            msg = new AffinityNotification<>(ctx.getAffinity(), ctx.getMessage());
        } else {
            msg = new Notification<>(ctx.getMessage());
        }

        if (callback != null) {
            callback.onComplete(null);
        }

        onAsyncEnqueue();

        ctx.getWorker().execute(() -> {
            onAsyncDequeue();

            doReceiveNotification(msg);
        });
    }

    @Override
    public void sendRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        RequestHandle<T> handle = registerRequest(ctx, callback);

        Request<T> msg;

        if (ctx.isStrictAffinity()) {
            msg = new AffinityRequest<>(ctx.getAffinity(), handle.getId(), ctx.getMessage());
        } else {
            msg = new Request<>(handle.getId(), ctx.getMessage());
        }

        onAsyncEnqueue();

        ctx.getWorker().execute(() -> {
            onAsyncDequeue();

            doReceiveRequest(msg, ctx.getWorker());
        });
    }

    @Override
    public void replyChunk(AffinityWorker worker, int requestId, T chunk, SendCallback callback) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            ResponseChunk<T> msg = new ResponseChunk<>(requestId, chunk);

            if (callback != null) {
                callback.onComplete(null);
            }

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveResponseChunk(handle, msg);
            });
        }
    }

    @Override
    public void reply(AffinityWorker worker, int requestId, T response, SendCallback callback) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            Response<T> msg = new Response<>(requestId, response);

            if (callback != null) {
                callback.onComplete(null);
            }

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveResponse(handle, msg);
            });
        }
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        discardRequests(t);
    }
}
