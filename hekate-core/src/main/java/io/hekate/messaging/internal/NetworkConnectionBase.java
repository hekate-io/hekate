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

import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinityStreamRequest;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.StreamRequest;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;

abstract class NetworkConnectionBase<T> extends MessagingConnectionBase<T> {
    private final NetworkEndpoint<MessagingProtocol> net;

    private final SendPressureGuard pressureGuard;

    public NetworkConnectionBase(NetworkEndpoint<MessagingProtocol> net, MessagingGateway<T> gateway, MessagingEndpoint<T> messaging) {
        super(gateway, gateway.getAsync(), messaging);

        assert net != null : "Endpoint is null.";

        this.net = net;
        this.pressureGuard = gateway.getSendPressureGuard();
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return net.disconnect();
    }

    @Override
    public void sendSingleRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        RequestHandle<T> handle = registerRequest(ctx, callback);

        Request<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityRequest<>(ctx.affinity(), handle.getId(), ctx.message());
        } else {
            msg = new Request<>(handle.getId(), ctx.message());
        }

        msg.prepareSend(handle, this);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void sendStreamRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        RequestHandle<T> handle = registerRequest(ctx, callback);

        StreamRequest<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityStreamRequest<>(ctx.affinity(), handle.getId(), ctx.message());
        } else {
            msg = new StreamRequest<>(handle.getId(), ctx.message());
        }

        msg.prepareSend(handle, this);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void sendNotification(MessageContext<T> ctx, SendCallback callback) {
        Notification<T> msg;

        if (ctx.hasAffinity()) {
            msg = new AffinityNotification<>(ctx.affinity(), ctx.message());
        } else {
            msg = new Notification<>(ctx.message());
        }

        msg.prepareSend(ctx.worker(), this, callback);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void replyChunk(MessagingWorker worker, int requestId, T chunk, SendCallback callback) {
        ResponseChunk<T> msg = new ResponseChunk<>(requestId, chunk);

        if (msg.prepareSend(worker, this, callback)) {
            net.send(msg, msg /* <-- Message itself is a callback.*/);
        }
    }

    @Override
    public void reply(MessagingWorker worker, int requestId, T response, SendCallback callback) {
        MessagingProtocol.FinalResponse<T> msg = new FinalResponse<>(requestId, response);

        msg.prepareSend(worker, this, callback);

        net.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    public SendPressureGuard getPressureGuard() {
        return pressureGuard;
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        net.disconnect();
    }
}
