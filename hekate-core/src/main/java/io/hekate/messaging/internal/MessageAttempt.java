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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinityVoidRequest;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidRequest;
import java.util.Optional;

class MessageAttempt<T> implements ClientSendContext {
    private final MessagingClient<T> client;

    private final ClusterTopology topology;

    private final MessageContext<T> ctx;

    private final Optional<FailureInfo> failure;

    private MessageMetaData metaData;

    public MessageAttempt(MessagingClient<T> client, ClusterTopology topology, MessageContext<T> ctx, Optional<FailureInfo> failure) {
        this.client = client;
        this.topology = topology;
        this.ctx = ctx;
        this.failure = failure;
    }

    public MessageContext<T> ctx() {
        return ctx;
    }

    public MessagingClient<T> client() {
        return client;
    }

    @Override
    public ClusterNode receiver() {
        return client.node();
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    @Override
    public boolean hasAffinity() {
        return ctx.hasAffinity();
    }

    @Override
    public int affinity() {
        return ctx.affinity();
    }

    @Override
    public Object affinityKey() {
        return ctx.affinityKey();
    }

    @Override
    public OutboundType type() {
        return ctx.type();
    }

    @Override
    public String channelName() {
        return ctx.opts().name();
    }

    @Override
    public MessageMetaData metaData() {
        if (metaData == null) {
            metaData = new MessageMetaData();
        }

        return metaData;
    }

    @Override
    public Object setAttribute(String name, Object value) {
        return ctx.setAttribute(name, value);
    }

    @Override
    public Object getAttribute(String name) {
        return ctx.getAttribute(name);
    }

    @Override
    public boolean hasMetaData() {
        return metaData != null && !metaData.isEmpty();
    }

    @Override
    public Optional<FailureInfo> failure() {
        return failure;
    }

    public MessageAttempt<T> newAttempt(Optional<FailureInfo> failure) {
        return new MessageAttempt<>(client, topology, ctx, failure);
    }

    public Notification<T> prepareNotification() {
        Notification<T> msg;

        T payload = interceptSend();

        MessageMetaData metaData = hasMetaData() ? metaData() : null;

        boolean isRetransmit = failure.isPresent();

        if (ctx.hasAffinity()) {
            msg = new AffinityNotification<>(
                ctx.affinity(),
                isRetransmit,
                ctx.opts().timeout(),
                payload,
                metaData
            );
        } else {
            msg = new Notification<>(
                isRetransmit,
                ctx.opts().timeout(),
                payload,
                metaData
            );
        }

        return msg;
    }

    public RequestBase<T> prepareRequest(int requestId) {
        T payload = interceptSend();

        MessageMetaData metaData = hasMetaData() ? metaData() : null;

        boolean isRetransmit = failure.isPresent();

        RequestBase<T> msg;

        if (ctx.hasAffinity()) {
            if (ctx.type() == OutboundType.SEND_WITH_ACK) {
                msg = new AffinityVoidRequest<>(
                    ctx.affinity(),
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            } else if (ctx.type() == OutboundType.SUBSCRIBE) {
                msg = new AffinitySubscribeRequest<>(
                    ctx.affinity(),
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            } else {
                msg = new AffinityRequest<>(
                    ctx.affinity(),
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            }
        } else {
            if (ctx.type() == OutboundType.SEND_WITH_ACK) {
                msg = new VoidRequest<>(
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            } else if (ctx.type() == OutboundType.SUBSCRIBE) {
                msg = new SubscribeRequest<>(
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            } else {
                msg = new Request<>(
                    requestId,
                    isRetransmit,
                    ctx.opts().timeout(),
                    payload,
                    metaData
                );
            }
        }

        return msg;
    }

    public T interceptReceive(T payload, ClientReceiveContext rsp) {
        return ctx.intercept().clientReceive(payload, rsp, this);
    }

    public void interceptReceiveError(MessagingException err) {
        ctx.intercept().clientReceiveError(err, this);
    }

    public void interceptReceiveVoid() {
        ctx.intercept().clientReceiveVoid(this);
    }

    private T interceptSend() {
        return ctx.intercept().clientSend(ctx.originalMessage(), this);
    }
}
