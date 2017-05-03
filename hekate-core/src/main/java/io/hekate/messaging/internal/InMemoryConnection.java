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
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribe;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.Subscribe;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;

class InMemoryConnection<T> extends MessagingConnectionBase<T> {
    public InMemoryConnection(MessagingGateway<T> gateway, MessagingExecutor async) {
        super(gateway, async, new DefaultMessagingEndpoint<>(gateway.id(), gateway.channel()));
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return null;
    }

    @Override
    public void sendNotification(MessageContext<T> ctx, SendCallback callback, boolean retransmit) {
        Notification<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityNotification<>(ctx.affinity(), retransmit, ctx.message());
        } else {
            msg = new Notification<>(retransmit, ctx.message());
        }

        if (callback != null) {
            callback.onComplete(null);
        }

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            doReceiveNotification(msg);
        });
    }

    @Override
    public void request(MessageContext<T> ctx, InternalRequestCallback<T> callback, boolean retransmit) {
        RequestHandle<T> handle = registerRequest(ctx, callback);

        Request<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityRequest<>(ctx.affinity(), handle.id(), retransmit, ctx.message());
        } else {
            msg = new Request<>(handle.id(), retransmit, ctx.message());
        }

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            doReceiveRequest(msg, ctx.worker());
        });
    }

    @Override
    public void subscribe(MessageContext<T> ctx, InternalRequestCallback<T> callback, boolean retransmit) {
        RequestHandle<T> handle = registerRequest(ctx, callback);

        Subscribe<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinitySubscribe<>(ctx.affinity(), handle.id(), retransmit, ctx.message());
        } else {
            msg = new Subscribe<>(handle.id(), retransmit, ctx.message());
        }

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            doReceiveRequest(msg, ctx.worker());
        });
    }

    @Override
    public void replyChunk(MessagingWorker worker, int requestId, T chunk, SendCallback callback) {
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
    public void reply(MessagingWorker worker, int requestId, T response, SendCallback callback) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            FinalResponse<T> msg = new MessagingProtocol.FinalResponse<>(requestId, response);

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
