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
import io.hekate.messaging.internal.MessagingProtocol.AffinityStreamRequest;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.StreamRequest;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;

class InMemoryConnection<T> extends MessagingConnectionBase<T> {
    public InMemoryConnection(MessagingGateway<T> gateway, MessagingExecutor async) {
        super(gateway, async, new DefaultMessagingEndpoint<>(gateway.localNode().id(), gateway.channel()));
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return null;
    }

    @Override
    public void sendNotification(MessageRoute<T> route, SendCallback callback, boolean retransmit) {
        MessageContext<T> ctx = route.ctx();

        Notification<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityNotification<>(ctx.affinity(), retransmit, route.preparePayload());
        } else {
            msg = new Notification<>(retransmit, route.preparePayload());
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
    public void request(MessageRoute<T> route, InternalRequestCallback<T> callback, boolean retransmit) {
        MessageContext<T> ctx = route.ctx();

        RequestHandle<T> handle = registerRequest(ctx, callback);

        Request<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityRequest<>(ctx.affinity(), handle.id(), retransmit, route.preparePayload());
        } else {
            msg = new Request<>(handle.id(), retransmit, route.preparePayload());
        }

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            doReceiveRequest(msg, ctx.worker());
        });
    }

    @Override
    public void stream(MessageRoute<T> route, InternalRequestCallback<T> callback, boolean retransmit) {
        MessageContext<T> ctx = route.ctx();

        RequestHandle<T> handle = registerRequest(ctx, callback);

        StreamRequest<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityStreamRequest<>(ctx.affinity(), handle.id(), retransmit, route.preparePayload());
        } else {
            msg = new StreamRequest<>(handle.id(), retransmit, route.preparePayload());
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
