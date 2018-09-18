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
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.RequestForResponseBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidResponse;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;

abstract class MessagingConnectionNetBase<T> extends MessagingConnectionBase<T> {
    private final NetworkEndpoint<MessagingProtocol> net;

    private final SendPressureGuard pressureGuard;

    public MessagingConnectionNetBase(NetworkEndpoint<MessagingProtocol> net, MessagingGatewayContext<T> ctx,
        MessagingEndpoint<T> endpoint) {
        super(ctx, ctx.async(), endpoint);

        assert net != null : "Network endpoint is null.";

        this.net = net;
        this.pressureGuard = ctx.sendGuard();
    }

    @Override
    public void request(MessageAttempt<T> attempt, InternalRequestCallback<T> callback) {
        RequestHandle<T> req = registerRequest(attempt, callback);

        RequestBase<T> msg = attempt.prepareRequest(req.id());

        msg.prepareSend(req, this);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void send(MessageAttempt<T> attempt, SendCallback callback) {
        Notification<T> msg = attempt.prepareNotification();

        msg.prepareSend(attempt.ctx().worker(), this, callback);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void replyChunk(MessagingWorker worker, T chunk, SubscribeRequest<T> request, SendCallback callback) {
        ResponseChunk<T> msg = new ResponseChunk<>(request.requestId(), chunk);

        if (msg.prepareSend(worker, this, pressureGuard(), request, callback)) {
            net.send(msg, msg /* <-- Message itself is a callback.*/);
        }
    }

    @Override
    public void replyFinal(MessagingWorker worker, T response, RequestForResponseBase<T> request, SendCallback callback) {
        FinalResponse<T> msg = new FinalResponse<>(request.requestId(), response);

        msg.prepareSend(worker, this, pressureGuard(), request, callback);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void replyVoid(MessagingWorker worker, RequestBase<T> request) {
        net.send(new VoidResponse(request.requestId()));
    }

    @Override
    public void replyError(MessagingWorker worker, int requestId, Throwable cause) {
        net.send(new ErrorResponse(requestId, ErrorUtils.stackTrace(cause)));
    }

    public SendPressureGuard pressureGuard() {
        return pressureGuard;
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        net.disconnect();
    }

    protected NetworkEndpoint<MessagingProtocol> net() {
        return net;
    }
}
