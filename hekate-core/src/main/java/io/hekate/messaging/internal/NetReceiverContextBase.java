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
import io.hekate.messaging.internal.MessagingProtocol.Conversation;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.Response;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;

abstract class NetReceiverContextBase<T> extends ReceiverContext<T> {
    private final NetworkEndpoint<MessagingProtocol> endpoint;

    private final SendBackPressure backPressure;

    public NetReceiverContextBase(NetworkEndpoint<MessagingProtocol> endpoint, MessagingGateway<T> gateway,
        MessagingEndpoint<T> messagingEndpoint, boolean trackIdleState) {
        super(gateway, gateway.getAsync(), messagingEndpoint, trackIdleState);

        assert endpoint != null : "Endpoint is null.";

        this.endpoint = endpoint;
        this.backPressure = gateway.getSendBackPressure();
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        return endpoint.disconnect();
    }

    @Override
    public void sendRequest(MessageContext<T> ctx, RequestCallback<T> callback) {
        // No need to call touch() since it is done in NetClientPool.

        RequestHolder<T> holder = registerRequest(ctx.getWorker(), ctx.getMessage(), callback);

        Request<T> msg;

        if (ctx.getAffinity() >= 0) {
            msg = new AffinityRequest<>(ctx.getAffinity(), holder.getId(), ctx.getMessage());
        } else {
            msg = new Request<>(holder.getId(), ctx.getMessage());
        }

        msg.prepareSend(ctx.getWorker(), this, callback);

        endpoint.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void sendNotification(MessageContext<T> ctx, SendCallback callback) {
        // No need to call touch() since it is done in NetClientPool.

        Notification<T> msg;

        if (ctx.getAffinity() >= 0) {
            msg = new AffinityNotification<>(ctx.getAffinity(), ctx.getMessage());
        } else {
            msg = new Notification<>(ctx.getMessage());
        }

        msg.prepareSend(ctx.getWorker(), this, callback);

        endpoint.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void replyConversation(AffinityWorker worker, int requestId, T conversation, RequestCallback<T> callback) {
        touch();

        RequestHolder<T> holder = registerRequest(worker, conversation, callback);

        Conversation<T> msg = new Conversation<>(requestId, holder.getId(), conversation);

        msg.prepareSend(worker, this, backPressure, callback);

        endpoint.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    public void replyChunk(AffinityWorker worker, int requestId, T chunk, SendCallback callback) {
        touch();

        ResponseChunk<T> msg = new ResponseChunk<>(requestId, chunk);

        if (msg.prepareSend(worker, this, backPressure, callback)) {
            endpoint.send(msg, msg /* <-- Message itself is a callback.*/);
        }
    }

    @Override
    public void reply(AffinityWorker worker, int requestId, T response, SendCallback callback) {
        touch();

        Response<T> msg = new Response<>(requestId, response);

        msg.prepareSend(worker, this, backPressure, callback);

        endpoint.send(msg, msg /* <-- Message itself is a callback.*/);
    }

    @Override
    protected void disconnectOnError(Throwable t) {
        endpoint.disconnect();
    }
}
