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

package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinityVoidRequest;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidRequest;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkFuture;

class MessagingConnectionMem<T> extends MessagingConnectionBase<T> {
    public MessagingConnectionMem(MessagingGatewayContext<T> ctx, MessagingExecutor async) {
        super(ctx, async, new DefaultMessagingEndpoint<>(ctx.localNode().address(), ctx.channel()));
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return null;
    }

    @Override
    public void send(MessageRoute<T> route, SendCallback callback, boolean retransmit) {
        MessageContext<T> ctx = route.ctx();

        Notification<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityNotification<>(ctx.affinity(), retransmit, ctx.opts().timeout(), route.preparePayload());
        } else {
            msg = new Notification<>(retransmit, ctx.opts().timeout(), route.preparePayload());
        }

        if (callback != null) {
            callback.onComplete(null);
        }

        long receivedAtMillis = msg.hasTimeout() ? System.nanoTime() : 0;

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            receiveNotificationAsync(msg, receivedAtMillis);
        });
    }

    @Override
    public void request(MessageRoute<T> route, InternalRequestCallback<T> callback, boolean retransmit) {
        MessageContext<T> ctx = route.ctx();

        RequestHandle<T> req = registerRequest(route, callback);

        RequestBase<T> msg;

        if (ctx.hasAffinity()) {
            if (ctx.type() == MessageContext.Type.VOID_REQUEST) {
                msg = new AffinityVoidRequest<>(ctx.affinity(), req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            } else if (ctx.type() == MessageContext.Type.SUBSCRIBE) {
                msg = new AffinitySubscribeRequest<>(ctx.affinity(), req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            } else {
                msg = new AffinityRequest<>(ctx.affinity(), req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            }
        } else {
            if (ctx.type() == MessageContext.Type.VOID_REQUEST) {
                msg = new VoidRequest<>(req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            } else if (ctx.type() == MessageContext.Type.SUBSCRIBE) {
                msg = new SubscribeRequest<>(req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            } else {
                msg = new Request<>(req.id(), retransmit, ctx.opts().timeout(), route.preparePayload());
            }
        }

        long receivedAtMillis = msg.hasTimeout() ? System.nanoTime() : 0;

        onAsyncEnqueue();

        ctx.worker().execute(() -> {
            onAsyncDequeue();

            receiveRequestAsync(msg, ctx.worker(), receivedAtMillis);
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
    public void replyFinal(MessagingWorker worker, int requestId, T response, SendCallback callback) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            FinalResponse<T> msg = new FinalResponse<>(requestId, response);

            if (callback != null) {
                callback.onComplete(null);
            }

            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveFinalResponse(handle, msg);
            });
        }
    }

    @Override
    public void replyVoid(MessagingWorker worker, int requestId) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveVoidResponse(handle);
            });
        }
    }

    @Override
    public void replyError(MessagingWorker worker, int requestId, Throwable cause) {
        RequestHandle<T> handle = requests().get(requestId);

        if (handle != null) {
            onAsyncEnqueue();

            worker.execute(() -> {
                onAsyncDequeue();

                doReceiveError(handle, new ErrorResponse(requestId, ErrorUtils.stackTrace(cause)));
            });
        }
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        discardRequests(t);
    }

    @Override
    protected int epoch() {
        return 0;
    }
}
