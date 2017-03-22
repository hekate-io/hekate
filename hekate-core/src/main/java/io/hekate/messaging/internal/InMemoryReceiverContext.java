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
import io.hekate.messaging.internal.MessagingProtocol.Conversation;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.Response;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;

class InMemoryReceiverContext<T> extends ReceiverContext<T> {
    public InMemoryReceiverContext(MessagingGateway<T> gateway, int poolOrder, int poolSize, AffinityExecutor async) {
        super(gateway, async, new DefaultMessagingEndpoint<>(gateway.getId(), poolOrder, poolSize, gateway.getChannel()), false);
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return null;
    }

    @Override
    public void sendNotification(MessageContext<T> ctx, SendCallback callback) {
        Notification<T> msg;

        if (ctx.getAffinity() >= 0) {
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
    public void sendRequest(MessageContext<T> ctx, RequestCallback<T> callback) {
        RequestHolder<T> holder = registerRequest(ctx.getWorker(), ctx.getMessage(), callback);

        Request<T> msg;

        if (ctx.getAffinity() >= 0) {
            msg = new AffinityRequest<>(ctx.getAffinity(), holder.getId(), ctx.getMessage());
        } else {
            msg = new Request<>(holder.getId(), ctx.getMessage());
        }

        onAsyncEnqueue();

        ctx.getWorker().execute(() -> {
            onAsyncDequeue();

            doReceiveRequest(msg, ctx.getWorker());
        });
    }

    @Override
    public void replyConversation(AffinityWorker worker, int requestId, T conversation, RequestCallback<T> callback) {
        RequestHolder<T> toHolder = unregisterRequest(requestId);

        if (toHolder != null) {
            RequestHolder<T> fromHolder = registerRequest(worker, conversation, callback);

            Conversation<T> msg = new Conversation<>(requestId, fromHolder.getId(), conversation);

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveConversation(msg, toHolder);
            });
        }
    }

    @Override
    public void replyChunk(AffinityWorker worker, int requestId, T chunk, SendCallback callback) {
        RequestHolder<T> holder = peekRequest(requestId);

        if (holder != null) {
            ResponseChunk<T> msg = new ResponseChunk<>(requestId, chunk);

            if (callback != null) {
                callback.onComplete(null);
            }

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveResponseChunk(msg, holder);
            });
        }
    }

    @Override
    public void reply(AffinityWorker worker, int requestId, T response, SendCallback callback) {
        RequestHolder<T> holder = unregisterRequest(requestId);

        if (holder != null) {
            Response<T> msg = new Response<>(requestId, response);

            if (callback != null) {
                callback.onComplete(null);
            }

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveResponse(msg, holder);
            });
        }
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        discardRequests(t);
    }
}
