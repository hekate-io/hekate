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
import io.hekate.messaging.unicast.Reply;
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

        RESPONSE_CHUNK,

        RESPONSE
    }

    static class Connect extends MessagingProtocol {
        private final ClusterNodeId to;

        private final MessagingChannelId channelId;

        private final int poolOrder;

        private final int poolSize;

        public Connect(ClusterNodeId to, MessagingChannelId channelId, int poolOrder, int poolSize) {
            this.to = to;
            this.channelId = channelId;
            this.poolOrder = poolOrder;
            this.poolSize = poolSize;
        }

        public ClusterNodeId getTo() {
            return to;
        }

        public MessagingChannelId getChannelId() {
            return channelId;
        }

        public int getPoolOrder() {
            return poolOrder;
        }

        public int getPoolSize() {
            return poolSize;
        }

        @Override
        public Type getType() {
            return Type.CONNECT;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    abstract static class NoReplyMessage<T> extends MessagingProtocol implements Message<T> {
        @Override
        public final boolean mustReply() {
            return false;
        }

        @Override
        public final void reply(T response) {
            throw new UnsupportedOperationException("Responses is not expected.");
        }

        @Override
        public final void reply(T response, SendCallback callback) {
            throw new UnsupportedOperationException("Responses is not expected.");
        }

        @Override
        public final void replyPartial(T response) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Responses is not expected.");
        }

        @Override
        public final void replyPartial(T response, SendCallback callback) throws UnsupportedOperationException {
            throw new UnsupportedOperationException("Responses is not expected.");
        }
    }

    static class Notification<T> extends NoReplyMessage<T> implements NetworkSendCallback<MessagingProtocol> {
        private final T payload;

        private AffinityWorker worker;

        private ReceiverContext<T> context;

        private SendCallback sendCallback;

        public Notification(T payload) {
            this.payload = payload;
        }

        public void prepareSend(AffinityWorker worker, ReceiverContext<T> context, SendCallback sendCallback) {
            this.worker = worker;
            this.context = context;
            this.sendCallback = sendCallback;
        }

        public void prepareReceive(ReceiverContext<T> context) {
            this.context = context;
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
        public MessagingEndpoint<T> getEndpoint() {
            return context.getEndpoint();
        }

        @Override
        public MessagingChannel<T> getChannel() {
            return context.getEndpoint().getChannel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (error.isPresent()) {
                context.notifyOnSendFailure(true, worker, payload, error.get(), sendCallback);
            } else {
                context.notifyOnSendSuccess(worker, payload, sendCallback);
            }
        }

        @Override
        public Type getType() {
            return Type.NOTIFICATION;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class AffinityNotification<T> extends Notification<T> {
        private final int affinity;

        public AffinityNotification(int affinity, T payload) {
            super(payload);

            this.affinity = affinity;
        }

        public int getAffinity() {
            return affinity;
        }

        @Override
        public Type getType() {
            return Type.AFFINITY_NOTIFICATION;
        }
    }

    static class Request<T> extends MessagingProtocol implements Message<T>, NetworkSendCallback<MessagingProtocol> {
        private static final AtomicIntegerFieldUpdater<Request> MUST_REPLY = newUpdater(Request.class, "mustReply");

        private final int requestId;

        private final T payload;

        private AffinityWorker worker;

        private ReceiverContext<T> context;

        private RequestHandle<T> handle;

        @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
        private volatile int mustReply;

        public Request(int requestId, T payload) {
            this.requestId = requestId;
            this.payload = payload;
        }

        public void prepareReceive(AffinityWorker worker, ReceiverContext<T> processor) {
            this.worker = worker;
            this.context = processor;
        }

        public void prepareSend(RequestHandle<T> handle, ReceiverContext<T> context) {
            this.worker = handle.getWorker();
            this.context = context;
            this.handle = handle;
        }

        public int getRequestId() {
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

            context.reply(worker, requestId, response, callback);
        }

        @Override
        public void replyPartial(T response) throws UnsupportedOperationException {
            replyPartial(response, null);
        }

        @Override
        public void replyPartial(T response, SendCallback callback) throws UnsupportedOperationException {
            checkNotResponded();

            context.replyChunk(worker, requestId, response, callback);
        }

        @Override
        public MessagingEndpoint<T> getEndpoint() {
            return context.getEndpoint();
        }

        @Override
        public MessagingChannel<T> getChannel() {
            return context.getEndpoint().getChannel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            error.ifPresent(err ->
                context.notifyOnReplyFailure(handle, err)
            );
        }

        @Override
        public Type getType() {
            return Type.REQUEST;
        }

        private void responded() {
            if (!MUST_REPLY.compareAndSet(this, 0, 1)) {
                throw new IllegalStateException("Message already responded [message=" + payload + ']');
            }
        }

        private void checkNotResponded() {
            if (!mustReply()) {
                throw new IllegalStateException("Message already responded.");
            }
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class AffinityRequest<T> extends Request<T> {
        private final int affinity;

        public AffinityRequest(int affinity, int requestId, T payload) {
            super(requestId, payload);

            this.affinity = affinity;
        }

        public int getAffinity() {
            return affinity;
        }

        @Override
        public Type getType() {
            return Type.AFFINITY_REQUEST;
        }
    }

    static class ResponseChunk<T> extends NoReplyMessage<T> implements Reply<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private final T payload;

        private AffinityWorker worker;

        private ReceiverContext<T> context;

        private T request;

        private SendBackPressure backPressure;

        private SendCallback sendCallback;

        public ResponseChunk(int requestId, T payload) {
            this.requestId = requestId;
            this.payload = payload;
        }

        public boolean prepareSend(AffinityWorker worker, ReceiverContext<T> context, SendBackPressure backPressure,
            SendCallback sendCallback) {
            this.worker = worker;
            this.context = context;
            this.backPressure = backPressure;
            this.sendCallback = sendCallback;

            // Apply back pressure when sending from server back to client.
            if (backPressure != null) {
                try {
                    backPressure.onEnqueue();
                } catch (InterruptedException | MessageQueueOverflowException e) {
                    backPressure.onDequeue();

                    context.notifyOnSendFailure(false, worker, payload, e, sendCallback);

                    return false;
                }
            }

            return true;
        }

        public void prepareReceive(ReceiverContext<T> context, T request) {
            this.context = context;
            this.request = request;
        }

        public int getRequestId() {
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
        public MessagingEndpoint<T> getEndpoint() {
            return context.getEndpoint();
        }

        @Override
        public MessagingChannel<T> getChannel() {
            return context.getEndpoint().getChannel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (backPressure != null) {
                backPressure.onDequeue();
            }

            if (error.isPresent()) {
                context.notifyOnSendFailure(true, worker, payload, error.get(), sendCallback);
            } else {
                context.notifyOnSendSuccess(worker, payload, sendCallback);
            }
        }

        @Override
        public Type getType() {
            return Type.RESPONSE_CHUNK;
        }

        @Override
        public T getRequest() {
            return request;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    static class Response<T> extends MessagingProtocol implements Reply<T>, NetworkSendCallback<MessagingProtocol> {
        private final int requestId;

        private final T payload;

        private AffinityWorker worker;

        private ReceiverContext<T> context;

        private T request;

        private SendBackPressure backPressure;

        private SendCallback sendCallback;

        public Response(int requestId, T payload) {
            this.requestId = requestId;
            this.payload = payload;
        }

        public void prepareSend(AffinityWorker worker, ReceiverContext<T> context, SendBackPressure backPressure, SendCallback onSend) {
            this.worker = worker;
            this.context = context;
            this.backPressure = backPressure;
            this.sendCallback = onSend;

            // Apply back pressure when sending from server back to client.
            if (backPressure != null) {
                // Note we are using IGNORE policy for back pressure
                // since we don't want this operation to fail/block but still want it to be counted by the back pressure guard.
                backPressure.onEnqueueIgnorePolicy();
            }
        }

        public void prepareReceive(ReceiverContext<T> context, T request) {
            this.context = context;
            this.request = request;
        }

        public int getRequestId() {
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
        public MessagingEndpoint<T> getEndpoint() {
            return context.getEndpoint();
        }

        @Override
        public MessagingChannel<T> getChannel() {
            return context.getEndpoint().getChannel();
        }

        @Override
        public void onComplete(MessagingProtocol message, Optional<Throwable> error, NetworkEndpoint<MessagingProtocol> endpoint) {
            if (backPressure != null) {
                backPressure.onDequeue();
            }

            if (error.isPresent()) {
                context.notifyOnSendFailure(true, worker, payload, error.get(), sendCallback);
            } else {
                context.notifyOnSendSuccess(worker, payload, sendCallback);
            }
        }

        @Override
        public Type getType() {
            return Type.RESPONSE;
        }

        @Override
        public T getRequest() {
            return request;
        }

        @Override
        public String toString() {
            return getClass().getSimpleName() + "[payload=" + payload + ']';
        }
    }

    public abstract Type getType();

    @SuppressWarnings("unchecked")
    public <T extends MessagingProtocol> T cast() {
        return (T)this;
    }
}
