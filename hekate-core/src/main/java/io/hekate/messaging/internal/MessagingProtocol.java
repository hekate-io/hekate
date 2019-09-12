/*
 * Copyright 2019 The Hekate Project
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

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.intercept.ClientOutboundContext;
import io.hekate.messaging.intercept.ClientReceiveContext;
import io.hekate.messaging.intercept.InboundType;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.intercept.ServerInboundContext;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import io.hekate.messaging.operation.ResponsePart;
import io.hekate.messaging.operation.SendCallback;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

abstract class MessagingProtocol {
    enum Type {
        CONNECT,

        NOTIFICATION,

        AFFINITY_NOTIFICATION,

        REQUEST,

        AFFINITY_REQUEST,

        VOID_REQUEST,

        AFFINITY_VOID_REQUEST,

        SUBSCRIBE,

        AFFINITY_SUBSCRIBE,

        RESPONSE_CHUNK,

        FINAL_RESPONSE,

        VOID_RESPONSE,

        ERROR_RESPONSE
    }

    static class Connect extends MessagingProtocol {
        private final ClusterNodeId to;

        private final ClusterAddress from;

        private final MessagingChannelId channelId;

        public Connect(ClusterNodeId to, ClusterAddress from, MessagingChannelId channelId) {
            this.to = to;
            this.from = from;
            this.channelId = channelId;
        }

        public ClusterNodeId to() {
            return to;
        }

        public ClusterAddress from() {
            return from;
        }

        public MessagingChannelId channelId() {
            return channelId;
        }

        @Override
        public Type messageType() {
            return Type.CONNECT;
        }
    }

    static class Notification<T> extends MessagingProtocol implements Message<T>, ServerReceiveContext<T> {
        private final long timeout;

        @ToStringIgnore
        private final boolean retransmit;

        @ToStringIgnore
        private final MessageMetaData metaData;

        private T payload;

        @ToStringIgnore
        private MessagingConnection<T> conn;

        @ToStringIgnore
        private Map<String, Object> attributes;

        public Notification(boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            this.retransmit = retransmit;
            this.timeout = timeout;
            this.metaData = metaData;
            this.payload = payload;
        }

        public void prepareSend(MessagingConnection<T> conn) {
            this.conn = conn;
        }

        public void prepareReceive(MessagingConnection<T> conn) {
            this.conn = conn;
        }

        public long timeout() {
            return timeout;
        }

        public boolean hasTimeout() {
            return timeout > 0;
        }

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public T payload() {
            return payload;
        }

        @Override
        public <P extends T> P payload(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public boolean isRetransmit() {
            return retransmit;
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public ClusterAddress from() {
            return conn.remoteAddress();
        }

        public boolean hasMetaData() {
            return metaData != null;
        }

        public MessageMetaData metaData() {
            return metaData;
        }

        @Override
        public Optional<MessageMetaData> readMetaData() {
            return Optional.ofNullable(metaData);
        }

        @Override
        public void overrideMessage(T msg) {
            ArgAssert.notNull(msg, "Message");

            this.payload = msg;
        }

        @Override
        public Object setAttribute(String name, Object value) {
            if (attributes == null) {
                attributes = new HashMap<>();
            }

            return attributes.put(name, value);
        }

        @Override
        public Object getAttribute(String name) {
            return attributes != null ? attributes.get(name) : null;
        }

        @Override
        public OutboundType type() {
            return OutboundType.SEND_NO_ACK;
        }

        @Override
        public String channelName() {
            return conn.endpoint().channel().name();
        }

        @Override
        public Type messageType() {
            return Type.NOTIFICATION;
        }

        @Override
        public final boolean mustReply() {
            return false;
        }

        @Override
        public boolean isSubscription() {
            return false;
        }

        @Override
        public boolean isRequest() {
            return false;
        }

        @Override
        public final void reply(T response) {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public final void reply(T response, SendCallback callback) {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public final void partialReply(T response) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public final void partialReply(T response, SendCallback callback) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }
    }

    static class AffinityNotification<T> extends Notification<T> {
        private final int affinity;

        public AffinityNotification(int affinity, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(retransmit, timeout, payload, metaData);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type messageType() {
            return Type.AFFINITY_NOTIFICATION;
        }
    }

    abstract static class RequestBase<T> extends MessagingProtocol implements Message<T>, ServerReceiveContext<T> {
        private final int requestId;

        private final boolean retransmit;

        private final long timeout;

        @ToStringIgnore
        private final MessageMetaData metaData;

        private T payload;

        @ToStringIgnore
        private MessagingWorker worker;

        @ToStringIgnore
        private MessagingConnection<T> conn;

        @ToStringIgnore
        private MessagingConnectionIn<T> connIn;

        @ToStringIgnore
        private Map<String, Object> attributes;

        public RequestBase(int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            this.requestId = requestId;
            this.retransmit = retransmit;
            this.timeout = timeout;
            this.payload = payload;
            this.metaData = metaData;
        }

        public abstract boolean isVoid();

        public void prepareSend(MessagingWorker worker, MessagingConnection<T> conn) {
            this.conn = conn;
            this.worker = worker;
        }

        public void prepareReceive(MessagingWorker worker, MessagingConnectionIn<T> conn) {
            this.worker = worker;
            this.conn = conn;
            this.connIn = conn;
        }

        public int requestId() {
            return requestId;
        }

        public long timeout() {
            return timeout;
        }

        public boolean hasTimeout() {
            return timeout > 0;
        }

        public boolean hasMetaData() {
            return metaData != null;
        }

        public MessageMetaData metaData() {
            return metaData;
        }

        @Override
        public void overrideMessage(T msg) {
            ArgAssert.notNull(msg, "Message");

            this.payload = msg;
        }

        @Override
        public Optional<MessageMetaData> readMetaData() {
            return Optional.ofNullable(metaData);
        }

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public boolean isRetransmit() {
            return retransmit;
        }

        @Override
        public T payload() {
            return payload;
        }

        @Override
        public <P extends T> P payload(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public ClusterAddress from() {
            return conn.remoteAddress();
        }

        @Override
        public Object setAttribute(String name, Object value) {
            if (attributes == null) {
                attributes = new HashMap<>();
            }

            return attributes.put(name, value);
        }

        @Override
        public Object getAttribute(String name) {
            return attributes != null ? attributes.get(name) : null;
        }

        @Override
        public String channelName() {
            return conn.endpoint().channel().name();
        }

        protected MessagingWorker worker() {
            return worker;
        }

        protected MessagingConnectionIn<T> connection() {
            return connIn;
        }
    }

    abstract static class RequestForResponseBase<T> extends RequestBase<T> {
        private static final AtomicIntegerFieldUpdater<RequestForResponseBase> MUST_REPLY = newUpdater(
            RequestForResponseBase.class,
            "mustReply"
        );

        @ToStringIgnore
        @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
        private volatile int mustReply;

        public RequestForResponseBase(int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);
        }

        @Override
        public boolean isRequest() {
            return true;
        }

        @Override
        public boolean mustReply() {
            return mustReply == 0;
        }

        @Override
        public void reply(T response) {
            reply(response, null);
        }

        @Override
        public void reply(T response, SendCallback callback) {
            responded();

            connection().replyFinal(worker(), response, this, callback);
        }

        private void responded() {
            if (!MUST_REPLY.compareAndSet(this, 0, 1)) {
                throw new IllegalStateException("Message already responded [message=" + payload() + ']');
            }
        }
    }

    static class Request<T> extends RequestForResponseBase<T> {
        public Request(int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);
        }

        @Override
        public boolean isVoid() {
            return false;
        }

        @Override
        public void partialReply(T response) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Partial replies are not supported by this message.");
        }

        @Override
        public void partialReply(T response, SendCallback callback) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Partial replies are not supported by this message.");
        }

        @Override
        public boolean isSubscription() {
            return false;
        }

        @Override
        public OutboundType type() {
            return OutboundType.REQUEST;
        }

        @Override
        public Type messageType() {
            return Type.REQUEST;
        }
    }

    static class AffinityRequest<T> extends Request<T> {
        private final int affinity;

        public AffinityRequest(int affinity, int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type messageType() {
            return Type.AFFINITY_REQUEST;
        }
    }

    static class SubscribeRequest<T> extends RequestForResponseBase<T> {
        public SubscribeRequest(int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);
        }

        @Override
        public boolean isVoid() {
            return false;
        }

        @Override
        public boolean isSubscription() {
            return true;
        }

        @Override
        public void partialReply(T response) {
            partialReply(response, null);
        }

        @Override
        public void partialReply(T response, SendCallback callback) {
            checkNotResponded();

            connection().replyChunk(worker(), response, this, callback);
        }

        @Override
        public OutboundType type() {
            return OutboundType.SUBSCRIBE;
        }

        @Override
        public Type messageType() {
            return Type.SUBSCRIBE;
        }

        private void checkNotResponded() {
            if (!mustReply()) {
                throw new IllegalStateException("Message already responded.");
            }
        }
    }

    static class AffinitySubscribeRequest<T> extends SubscribeRequest<T> {
        private final int affinity;

        public AffinitySubscribeRequest(
            int affinity,
            int requestId,
            boolean retransmit,
            long timeout,
            T payload,
            MessageMetaData metaData
        ) {
            super(requestId, retransmit, timeout, payload, metaData);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type messageType() {
            return Type.AFFINITY_SUBSCRIBE;
        }
    }

    static class VoidRequest<T> extends RequestBase<T> {
        public VoidRequest(int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);
        }

        @Override
        public boolean isVoid() {
            return true;
        }

        @Override
        public boolean isSubscription() {
            return false;
        }

        @Override
        public boolean isRequest() {
            return false;
        }

        @Override
        public boolean mustReply() {
            return false;
        }

        @Override
        public void reply(T response) throws UnsupportedOperationException, IllegalStateException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public void reply(T response, SendCallback callback) throws UnsupportedOperationException, IllegalStateException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public void partialReply(T response) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public void partialReply(T response, SendCallback callback) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Reply is not supported by this message.");
        }

        @Override
        public OutboundType type() {
            return OutboundType.SEND_WITH_ACK;
        }

        @Override
        public Type messageType() {
            return Type.VOID_REQUEST;
        }
    }

    static class AffinityVoidRequest<T> extends VoidRequest<T> {
        private final int affinity;

        public AffinityVoidRequest(int affinity, int requestId, boolean retransmit, long timeout, T payload, MessageMetaData metaData) {
            super(requestId, retransmit, timeout, payload, metaData);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type messageType() {
            return Type.AFFINITY_VOID_REQUEST;
        }
    }

    static class ResponseChunk<T> extends MessagingProtocol implements ResponsePart<T>, ServerSendContext<T>, ClientReceiveContext<T> {
        private final int requestId;

        private T payload;

        @ToStringIgnore
        private MessageMetaData metaData;

        @ToStringIgnore
        private MessagingConnection<T> conn;

        @ToStringIgnore
        private ClientOutboundContext<T> clientCtx;

        @ToStringIgnore
        private ServerInboundContext<T> serverCtx;

        public ResponseChunk(int requestId, T payload) {
            this(requestId, payload, null);
        }

        public ResponseChunk(int requestId, T payload, MessageMetaData metaData) {
            this.requestId = requestId;
            this.payload = payload;
            this.metaData = metaData;
        }

        public void prepareSend(MessagingConnection<T> conn, ServerReceiveContext<T> serverCtx) {
            this.conn = conn;
            this.serverCtx = serverCtx;
        }

        public void prepareReceive(MessagingConnection<T> conn, ClientOutboundContext<T> attempt) {
            this.conn = conn;
            this.clientCtx = attempt;
        }

        public int requestId() {
            return requestId;
        }

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public T payload() {
            return payload;
        }

        @Override
        public <P extends T> P payload(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public ServerInboundContext<T> inboundContext() {
            return serverCtx;
        }

        @Override
        public void overrideMessage(T msg) {
            ArgAssert.notNull(msg, "Message");

            this.payload = msg;
        }

        @Override
        public ClientOutboundContext<T> outboundContext() {
            return clientCtx;
        }

        @Override
        public boolean isLastPart() {
            return false;
        }

        @Override
        public ClusterTopology topology() {
            return clientCtx.topology();
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public T request() {
            return clientCtx.payload();
        }

        @Override
        public <P extends T> P request(Class<P> type) {
            return type.cast(request());
        }

        @Override
        public InboundType type() {
            return InboundType.RESPONSE_CHUNK;
        }

        @Override
        public Optional<MessageMetaData> readMetaData() {
            return Optional.ofNullable(metaData);
        }

        @Override
        public boolean hasMetaData() {
            return metaData != null;
        }

        @Override
        public MessageMetaData metaData() {
            if (metaData == null) {
                metaData = new MessageMetaData();
            }

            return metaData;
        }

        @Override
        public Type messageType() {
            return Type.RESPONSE_CHUNK;
        }
    }

    static class FinalResponse<T> extends ResponseChunk<T> {
        public FinalResponse(int requestId, T payload) {
            super(requestId, payload);
        }

        public FinalResponse(int requestId, T payload, MessageMetaData metaData) {
            super(requestId, payload, metaData);
        }

        @Override
        public boolean isLastPart() {
            return true;
        }

        @Override
        public InboundType type() {
            return InboundType.FINAL_RESPONSE;
        }

        @Override
        public Type messageType() {
            return Type.FINAL_RESPONSE;
        }
    }

    static class VoidResponse extends MessagingProtocol {
        private final int requestId;

        public VoidResponse(int requestId) {
            this.requestId = requestId;
        }

        public int requestId() {
            return requestId;
        }

        @Override
        public Type messageType() {
            return Type.VOID_RESPONSE;
        }
    }

    static class ErrorResponse extends MessagingProtocol {
        private final int requestId;

        @ToStringIgnore
        private final String stackTrace;

        public ErrorResponse(int requestId, String stackTrace) {
            this.requestId = requestId;
            this.stackTrace = stackTrace;
        }

        public int requestId() {
            return requestId;
        }

        public String stackTrace() {
            return stackTrace;
        }

        @Override
        public Type messageType() {
            return Type.ERROR_RESPONSE;
        }
    }

    public abstract Type messageType();

    @SuppressWarnings("unchecked")
    public <T extends MessagingProtocol> T cast() {
        return (T)this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
