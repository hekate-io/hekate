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

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.codec.CodecException;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkSendCallback;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
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
            super(false);

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
        public Type type() {
            return Type.CONNECT;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    abstract static class NoReplyMessage<T> extends MessagingProtocol implements Message<T> {
        public NoReplyMessage(boolean retransmit) {
            super(retransmit);
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

    static class Notification<T> extends NoReplyMessage<T> implements NetworkSendCallback<MessagingProtocol> {
        private final long timeout;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private SendCallback callback;

        public Notification(boolean retransmit, long timeout, T payload) {
            super(retransmit);

            this.timeout = timeout;
            this.payload = payload;
        }

        public void prepareSend(MessagingWorker worker, MessagingConnectionBase<T> conn, SendCallback callback) {
            this.worker = worker;
            this.conn = conn;
            this.callback = callback;
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (error.isPresent()) {
                conn.notifyOnSendFailure(worker, payload, error.get(), callback);
            } else {
                conn.notifyOnSendSuccess(worker, payload, callback);
            }
        }

        public void prepareReceive(MessagingConnectionBase<T> conn) {
            this.conn = conn;

            this.payload = conn.prepareInbound(this.payload);
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
        public T get() {
            return payload;
        }

        @Override
        public <P extends T> P get(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public Type type() {
            return Type.NOTIFICATION;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class AffinityNotification<T> extends Notification<T> {
        private final int affinity;

        public AffinityNotification(int affinity, boolean retransmit, long timeout, T payload) {
            super(retransmit, timeout, payload);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type type() {
            return Type.AFFINITY_NOTIFICATION;
        }
    }

    abstract static class RequestBase<T> extends MessagingProtocol implements Message<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private final long timeout;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private RequestHandle<T> handle;

        public RequestBase(int requestId, boolean retransmit, long timeout, T payload) {
            super(retransmit);

            this.requestId = requestId;
            this.timeout = timeout;
            this.payload = payload;
        }

        public abstract boolean isVoid();

        public void prepareReceive(MessagingWorker worker, MessagingConnectionBase<T> conn) {
            this.worker = worker;
            this.conn = conn;

            this.payload = conn.prepareInbound(this.payload);
        }

        public void prepareSend(RequestHandle<T> handle, MessagingConnectionBase<T> conn) {
            this.worker = handle.worker();
            this.conn = conn;
            this.handle = handle;
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (error.isPresent()) {
                conn.notifyOnRequestFailure(handle, error.get());
            }
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

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public T get() {
            return payload;
        }

        @Override
        public <P extends T> P get(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        protected MessagingWorker worker() {
            return worker;
        }

        protected MessagingConnectionBase<T> connection() {
            return conn;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    abstract static class RequestWithResponseBase<T> extends RequestBase<T> {
        private static final AtomicIntegerFieldUpdater<RequestWithResponseBase> MUST_REPLY = newUpdater(
            RequestWithResponseBase.class,
            "mustReply"
        );

        @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
        private volatile int mustReply;

        public RequestWithResponseBase(int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);
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

            T transformed = connection().prepareReply(response);

            connection().replyFinal(worker(), requestId(), transformed, callback);
        }

        private void responded() {
            if (!MUST_REPLY.compareAndSet(this, 0, 1)) {
                throw new IllegalStateException("Message already responded [message=" + get() + ']');
            }
        }
    }

    static class Request<T> extends RequestWithResponseBase<T> {
        public Request(int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);
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
        public Type type() {
            return Type.REQUEST;
        }
    }

    static class AffinityRequest<T> extends Request<T> {
        private final int affinity;

        public AffinityRequest(int affinity, int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type type() {
            return Type.AFFINITY_REQUEST;
        }
    }

    static class SubscribeRequest<T> extends RequestWithResponseBase<T> {
        public SubscribeRequest(int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);
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

            T transformed = connection().prepareReply(response);

            connection().replyChunk(worker(), requestId(), transformed, callback);
        }

        @Override
        public Type type() {
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

        public AffinitySubscribeRequest(int affinity, int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type type() {
            return Type.AFFINITY_SUBSCRIBE;
        }
    }

    static class VoidRequest<T> extends RequestBase<T> {
        public VoidRequest(int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);
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
        public Type type() {
            return Type.VOID_REQUEST;
        }
    }

    static class AffinityVoidRequest<T> extends VoidRequest<T> {
        private final int affinity;

        public AffinityVoidRequest(int affinity, int requestId, boolean retransmit, long timeout, T payload) {
            super(requestId, retransmit, timeout, payload);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type type() {
            return Type.AFFINITY_VOID_REQUEST;
        }
    }

    static class ResponseChunk<T> extends NoReplyMessage<T> implements Response<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private MessageRoute<T> route;

        private SendPressureGuard backPressure;

        private SendCallback callback;

        public ResponseChunk(int requestId, T payload) {
            super(false);

            this.requestId = requestId;
            this.payload = payload;
        }

        public boolean prepareSend(MessagingWorker worker, MessagingConnectionNetBase<T> conn, SendCallback callback) {
            this.worker = worker;
            this.conn = conn;
            this.callback = callback;
            this.backPressure = conn.pressureGuard();

            // Apply back pressure when sending from server back to client.
            if (backPressure != null) {
                try {
                    backPressure.onEnqueue();
                } catch (InterruptedException | MessageQueueOverflowException e) {
                    backPressure.onDequeue();

                    conn.notifyOnSendFailure(worker, payload, e, callback);

                    return false;
                }
            }

            return true;
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (backPressure != null) {
                backPressure.onDequeue();
            }

            if (error.isPresent()) {
                conn.notifyOnSendFailure(worker, payload, error.get(), callback);
            } else {
                conn.notifyOnSendSuccess(worker, payload, callback);
            }
        }

        public void prepareReceive(MessagingConnectionBase<T> conn, MessageRoute<T> route) {
            this.conn = conn;
            this.route = route;

            this.payload = conn.prepareInbound(this.payload);
        }

        public int requestId() {
            return requestId;
        }

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public T get() {
            return payload;
        }

        @Override
        public <P extends T> P get(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public boolean isPartial() {
            return true;
        }

        @Override
        public ClusterTopology topology() {
            return route.topology();
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public Type type() {
            return Type.RESPONSE_CHUNK;
        }

        @Override
        public T request() {
            return route.ctx().originalMessage();
        }

        @Override
        public <P extends T> P request(Class<P> type) {
            return type.cast(request());
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class FinalResponse<T> extends MessagingProtocol implements Response<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private MessageRoute<T> route;

        private SendPressureGuard backPressure;

        private SendCallback callback;

        public FinalResponse(int requestId, T payload) {
            super(false);

            this.requestId = requestId;
            this.payload = payload;
        }

        public void prepareSend(MessagingWorker worker, MessagingConnectionNetBase<T> conn, SendCallback callback) {
            this.worker = worker;
            this.conn = conn;
            this.callback = callback;
            this.backPressure = conn.pressureGuard();

            // Apply back pressure when sending from server back to client.
            if (backPressure != null) {
                // Note we are using IGNORE policy for back pressure
                // since we don't want this operation to fail/block but still want it to be counted by the back pressure guard.
                backPressure.onEnqueueIgnorePolicy();
            }
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (backPressure != null) {
                backPressure.onDequeue();
            }

            if (error.isPresent()) {
                Throwable cause = error.get();

                if (cause instanceof CodecException) {
                    conn.replyError(worker, requestId, cause);
                }

                conn.notifyOnSendFailure(worker, payload, cause, callback);
            } else {
                conn.notifyOnSendSuccess(worker, payload, callback);
            }
        }

        public void prepareReceive(MessagingConnectionBase<T> conn, MessageRoute<T> route) {
            this.conn = conn;
            this.route = route;

            this.payload = conn.prepareInbound(this.payload);
        }

        public int requestId() {
            return requestId;
        }

        @Override
        public T get() {
            return payload;
        }

        @Override
        public <P extends T> P get(Class<P> type) {
            return type.cast(payload);
        }

        @Override
        public boolean is(Class<? extends T> type) {
            return type.isInstance(payload);
        }

        @Override
        public boolean isPartial() {
            return false;
        }

        @Override
        public ClusterTopology topology() {
            return route.topology();
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public Type type() {
            return Type.FINAL_RESPONSE;
        }

        @Override
        public T request() {
            return route.ctx().originalMessage();
        }

        @Override
        public <P extends T> P request(Class<P> type) {
            return type.cast(request());
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class VoidResponse extends MessagingProtocol {
        private final int requestId;

        public VoidResponse(int requestId) {
            super(false);

            this.requestId = requestId;
        }

        public int requestId() {
            return requestId;
        }

        @Override
        public Type type() {
            return Type.VOID_RESPONSE;
        }
    }

    static class ErrorResponse extends MessagingProtocol {
        private final int requestId;

        @ToStringIgnore
        private final String stackTrace;

        public ErrorResponse(int requestId, String stackTrace) {
            super(false);

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
        public Type type() {
            return Type.ERROR_RESPONSE;
        }
    }

    private final boolean retransmit;

    public MessagingProtocol(boolean retransmit) {
        this.retransmit = retransmit;
    }

    public abstract Type type();

    public boolean isRetransmit() {
        return retransmit;
    }

    @SuppressWarnings("unchecked")
    public <T extends MessagingProtocol> T cast() {
        return (T)this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
