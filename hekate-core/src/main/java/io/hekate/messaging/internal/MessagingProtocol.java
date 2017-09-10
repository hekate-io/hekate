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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.unicast.Response;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkSendCallback;
import io.hekate.util.format.ToString;
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

        STREAM,

        AFFINITY_STREAM,

        RESPONSE_CHUNK,

        FINAL_RESPONSE
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
        public boolean isStream() {
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

    abstract static class RequestBase<T> extends MessagingProtocol implements Message<T>, NetworkSendCallback<MessagingProtocol> {
        private static final AtomicIntegerFieldUpdater<RequestBase> MUST_REPLY = newUpdater(RequestBase.class, "mustReply");

        private final int requestId;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private RequestHandle<T> handle;

        @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
        private volatile int mustReply;

        public RequestBase(int requestId, boolean retransmit, T payload) {
            super(retransmit);

            this.requestId = requestId;
            this.payload = payload;
        }

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

        public int requestId() {
            return requestId;
        }

        @Override
        public boolean isRequest() {
            return true;
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

            T transformed = conn.prepareReply(response);

            conn.reply(worker, requestId, transformed, callback);
        }

        @Override
        public void partialReply(T response) throws UnsupportedOperationException {
            partialReply(response, null);
        }

        @Override
        public void partialReply(T response, SendCallback callback) throws UnsupportedOperationException {
            checkNotResponded();

            T transformed = conn.prepareReply(response);

            conn.replyChunk(worker, requestId, transformed, callback);
        }

        @Override
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public MessagingChannel<T> channel() {
            return conn.endpoint().channel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            error.ifPresent(err ->
                conn.notifyOnReplyFailure(handle, err)
            );
        }

        protected void checkNotResponded() {
            if (!mustReply()) {
                throw new IllegalStateException("Message already responded.");
            }
        }

        private void responded() {
            if (!MUST_REPLY.compareAndSet(this, 0, 1)) {
                throw new IllegalStateException("Message already responded [message=" + payload + ']');
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class Connect extends MessagingProtocol {
        private final ClusterNodeId to;

        private final MessagingChannelId channelId;

        public Connect(ClusterNodeId to, MessagingChannelId channelId) {
            super(false);

            this.to = to;
            this.channelId = channelId;
        }

        public ClusterNodeId to() {
            return to;
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

    static class Notification<T> extends NoReplyMessage<T> implements NetworkSendCallback<MessagingProtocol> {
        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private SendCallback callback;

        public Notification(boolean retransmit, T payload) {
            super(retransmit);

            this.payload = payload;
        }

        public void prepareSend(MessagingWorker worker, MessagingConnectionBase<T> conn, SendCallback callback) {
            this.worker = worker;
            this.conn = conn;
            this.callback = callback;
        }

        public void prepareReceive(MessagingConnectionBase<T> conn) {
            this.conn = conn;

            this.payload = conn.prepareInbound(this.payload);
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
        public MessagingChannel<T> channel() {
            return conn.endpoint().channel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (error.isPresent()) {
                conn.notifyOnSendFailure(worker, payload, error.get(), callback);
            } else {
                conn.notifyOnSendSuccess(worker, payload, callback);
            }
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

        public AffinityNotification(int affinity, boolean retransmit, T payload) {
            super(retransmit, payload);

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

    static class Request<T> extends RequestBase<T> {
        public Request(int requestId, boolean retransmit, T payload) {
            super(requestId, retransmit, payload);
        }

        @Override
        public void partialReply(T response, SendCallback callback) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Partial replies are not supported by this message.");
        }

        @Override
        public boolean isStream() {
            return false;
        }

        @Override
        public Type type() {
            return Type.REQUEST;
        }
    }

    static class AffinityRequest<T> extends Request<T> {
        private final int affinity;

        public AffinityRequest(int affinity, int requestId, boolean retransmit, T payload) {
            super(requestId, retransmit, payload);

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

    static class StreamRequest<T> extends RequestBase<T> {
        public StreamRequest(int requestId, boolean retransmit, T payload) {
            super(requestId, retransmit, payload);
        }

        @Override
        public boolean isStream() {
            return true;
        }

        @Override
        public Type type() {
            return Type.STREAM;
        }
    }

    static class AffinityStreamRequest<T> extends StreamRequest<T> {
        private final int affinity;

        public AffinityStreamRequest(int affinity, int requestId, boolean retransmit, T payload) {
            super(requestId, retransmit, payload);

            this.affinity = affinity;
        }

        public int affinity() {
            return affinity;
        }

        @Override
        public Type type() {
            return Type.AFFINITY_STREAM;
        }
    }

    static class ResponseChunk<T> extends NoReplyMessage<T>
        implements Response<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private T payload;

        private MessagingWorker worker;

        private MessagingConnectionBase<T> conn;

        private T request;

        private SendPressureGuard backPressure;

        private SendCallback callback;

        public ResponseChunk(int requestId, T payload) {
            super(false);

            this.requestId = requestId;
            this.payload = payload;
        }

        public boolean prepareSend(MessagingWorker worker, NetworkConnectionBase<T> conn, SendCallback callback) {
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

        public void prepareReceive(MessagingConnectionBase<T> conn, T request) {
            this.conn = conn;
            this.request = request;

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
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public MessagingChannel<T> channel() {
            return conn.endpoint().channel();
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

        @Override
        public Type type() {
            return Type.RESPONSE_CHUNK;
        }

        @Override
        public T request() {
            return request;
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

        private T request;

        private SendPressureGuard backPressure;

        private SendCallback callback;

        public FinalResponse(int requestId, T payload) {
            super(false);

            this.requestId = requestId;
            this.payload = payload;
        }

        public void prepareSend(MessagingWorker worker, NetworkConnectionBase<T> conn, SendCallback callback) {
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

        public void prepareReceive(MessagingConnectionBase<T> conn, T request) {
            this.conn = conn;
            this.request = request;

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
        public MessagingEndpoint<T> endpoint() {
            return conn.endpoint();
        }

        @Override
        public MessagingChannel<T> channel() {
            return conn.endpoint().channel();
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

        @Override
        public Type type() {
            return Type.FINAL_RESPONSE;
        }

        @Override
        public T request() {
            return request;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
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
}
