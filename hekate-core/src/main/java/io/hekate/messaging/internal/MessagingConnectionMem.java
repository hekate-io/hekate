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
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.RequestForResponseBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
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
    public void send(MessageAttempt<T> attempt, SendCallback callback) {
        Notification<T> msg = attempt.prepareNotification();

        if (callback != null) {
            callback.onComplete(null);
        }

        long receivedAtMillis = msg.hasTimeout() ? System.nanoTime() : 0;

        onAsyncEnqueue();

        attempt.ctx().worker().execute(() -> {
            onAsyncDequeue();

            receiveNotificationAsync(msg, receivedAtMillis);
        });
    }

    @Override
    public void request(MessageAttempt<T> attempt, InternalRequestCallback<T> callback) {
        RequestHandle<T> req = registerRequest(attempt, callback);

        RequestBase<T> msg = attempt.prepareRequest(req.id());

        long receivedAtMillis = msg.hasTimeout() ? System.nanoTime() : 0;

        onAsyncEnqueue();

        attempt.ctx().worker().execute(() -> {
            onAsyncDequeue();

            receiveRequestAsync(msg, attempt.ctx().worker(), receivedAtMillis);
        });
    }

    @Override
    public void replyChunk(MessagingWorker worker, T chunk, SubscribeRequest<T> request, SendCallback callback) {
        RequestHandle<T> handle = requests().get(request.requestId());

        if (handle != null) {
            ResponseChunk<T> msg = new ResponseChunk<>(request.requestId(), chunk);

            msg.prepareSend(worker, this, null /* <- no back pressure for in-memory */, request, callback);

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
    public void replyFinal(MessagingWorker worker, T response, RequestForResponseBase<T> request, SendCallback callback) {
        RequestHandle<T> handle = requests().get(request.requestId());

        if (handle != null) {
            FinalResponse<T> msg = new FinalResponse<>(request.requestId(), response);

            msg.prepareSend(worker, this, null /* <- no back pressure for in-memory */, request, callback);

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
    public void replyVoid(MessagingWorker worker, RequestBase<T> request) {
        RequestHandle<T> handle = requests().get(request.requestId());

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
