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

import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.internal.MessagingProtocol.Conversation;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.Response;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.unicast.RequestCallback;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ReceiverContext<T> {
    static class RequestHolder<T> {
        private final int id;

        private final AffinityWorker worker;

        private final T message;

        private final RequestCallback<T> callback;

        private final int epoch;

        public RequestHolder(int id, AffinityWorker worker, T message, int epoch, RequestCallback<T> callback) {
            this.id = id;
            this.worker = worker;
            this.message = message;
            this.epoch = epoch;
            this.callback = callback;
        }

        public int getId() {
            return id;
        }

        public AffinityWorker getWorker() {
            return worker;
        }

        public T getMessage() {
            return message;
        }

        public int getEpoch() {
            return epoch;
        }

        public RequestCallback<T> getCallback() {
            return callback;
        }
    }

    private static final int REQUEST_MAP_INIT_CAPACITY = 128;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Map<Integer, RequestHolder<T>> requests = new ConcurrentHashMap<>(REQUEST_MAP_INIT_CAPACITY);

    private final MessagingGateway<T> gateway;

    private final MessageReceiver<T> receiver;

    private final AtomicInteger idGen = new AtomicInteger();

    private final AffinityExecutor async;

    private final MetricsCallback metrics;

    private final ReceiveBackPressure backPressure;

    private final MessagingEndpoint<T> endpoint;

    private final boolean trackIdleState;

    private volatile boolean touched;

    private volatile int epoch;

    public ReceiverContext(MessagingGateway<T> gateway, AffinityExecutor async, MessagingEndpoint<T> endpoint, boolean trackIdleState) {
        assert gateway != null : "Gateway is null.";
        assert async != null : "Executor is null.";
        assert endpoint != null : "Messaging endpoint is null.";

        this.gateway = gateway;
        this.async = async;
        this.receiver = gateway.getReceiver();
        this.endpoint = endpoint;
        this.metrics = gateway.getMetrics();
        this.backPressure = gateway.getReceiveBackPressure();
        this.trackIdleState = trackIdleState;
    }

    public abstract NetworkFuture<MessagingProtocol> disconnect();

    public abstract void sendNotification(MessageContext<T> ctx, SendCallback callback);

    public abstract void sendRequest(MessageContext<T> ctx, RequestCallback<T> callback);

    public abstract void replyConversation(AffinityWorker worker, int requestId, T conversation, RequestCallback<T> callback);

    public abstract void replyChunk(AffinityWorker worker, int requestId, T chunk, SendCallback callback);

    public abstract void reply(AffinityWorker worker, int requestId, T response, SendCallback callback);

    protected abstract void disconnectOnError(Throwable t);

    public MessagingGateway<T> getGateway() {
        return gateway;
    }

    public void receive(NetworkMessage<MessagingProtocol> netMsg, NetworkEndpoint<MessagingProtocol> from) {
        touch();

        try {
            MessagingProtocol.Type msgType = MessagingProtocolCodec.previewType(netMsg);

            switch (msgType) {
                case NOTIFICATION: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected message [channel={}, message={}]", gateway.getName(), netMsg);
                        }
                    } else {
                        if (async.isAsync()) {
                            int affinity = randomAffinity();

                            AffinityWorker worker = async.workerFor(affinity);

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveNotification(m.cast());
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveNotification(netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case AFFINITY_NOTIFICATION: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected message [channel={}, message={}]", gateway.getName(), netMsg);
                        }
                    } else {
                        if (async.isAsync()) {
                            int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                            AffinityWorker worker = async.workerFor(affinity);

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveNotification(m.cast());
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveNotification(netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case REQUEST: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected message [channel={}, message={}]", gateway.getName(), netMsg);
                        }
                    } else {
                        int affinity = randomAffinity();

                        AffinityWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveRequest(m.cast(), worker);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveRequest(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case AFFINITY_REQUEST: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected message [channel={}, message={}]", gateway.getName(), netMsg);
                        }
                    } else {
                        int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                        AffinityWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveRequest(m.cast(), worker);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveRequest(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHolder<T> holder = unregisterRequest(requestId);

                    if (holder != null) {
                        if (async.isAsync()) {
                            AffinityWorker worker = holder.getWorker();

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveResponse(m.cast(), holder);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveResponse(netMsg.decode().cast(), holder);
                        }
                    }

                    break;
                }
                case CONVERSATION: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHolder<T> holder = unregisterRequest(requestId);

                    if (holder != null) {
                        if (async.isAsync()) {
                            AffinityWorker worker = holder.getWorker();

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveConversation(m.cast(), holder);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveConversation(netMsg.decode().cast(), holder);
                        }
                    }

                    break;
                }
                case RESPONSE_CHUNK: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHolder<T> holder = peekRequest(requestId);

                    if (holder != null) {
                        if (async.isAsync()) {
                            AffinityWorker worker = holder.getWorker();

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveResponseChunk(m.cast(), holder);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveResponseChunk(netMsg.decode().cast(), holder);
                        }
                    }

                    break;
                }
                case CONNECT: // Connect message is not expected here.
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + msgType);
                }
            }
        } catch (IOException e) {
            log.error("Failed to decode network message [message={}]", netMsg, e);

            disconnectOnError(e);
        }
    }

    public void notifyOnSendSuccess(AffinityWorker worker, T payload, SendCallback callback) {
        if (callback != null) {
            if (async.isAsync()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnSendSuccess(payload, callback);
                });
            } else {
                doNotifyOnSendSuccess(payload, callback);
            }
        }
    }

    public void notifyOnReplyFailure(AffinityWorker worker, int responseId, T payload, Throwable error, RequestCallback<T> callback) {
        if (discardRequest(responseId)) {
            if (async.isAsync() && !worker.isShutdown()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnReplyFailure(payload, error, callback);
                });
            } else {
                doNotifyOnReplyFailure(payload, error, callback);
            }
        }
    }

    public void notifyOnSendFailure(boolean disconnect, AffinityWorker worker, T payload, Throwable error, SendCallback callback) {
        if (callback != null) {
            if (async.isAsync() && !worker.isShutdown()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnSendFailure(disconnect, payload, error, callback);
                });
            } else {
                doNotifyOnSendFailure(disconnect, payload, error, callback);
            }
        }
    }

    public boolean discardRequest(int id) {
        return unregisterRequest(id) != null;
    }

    public void discardRequests(Throwable cause) {
        discardRequests(epoch, cause);
    }

    public void discardRequests(int epoch, Throwable cause) {
        List<RequestHolder<T>> failed = new ArrayList<>(requests.size());

        for (RequestHolder<T> holder : requests.values()) {
            if (holder.getEpoch() == epoch) {
                if (requests.remove(holder.getId()) != null) {
                    failed.add(holder);
                }
            }
        }

        onRequestUnregister(failed.size());

        for (RequestHolder<T> holder : failed) {
            AffinityWorker worker = holder.getWorker();

            if (async.isAsync()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doDiscardRequest(cause, holder);
                });
            } else {
                doDiscardRequest(cause, holder);
            }
        }
    }

    public boolean isIdle() {
        // Check if was touched since the previous check (volatile read).
        boolean idle = !touched;

        if (idle) {
            // Do not mark as idle if there are requests awaiting for responses.
            if (!requests.isEmpty()) {
                idle = false;
            }
        } else {
            // Reset touched flag (volatile write).
            // Note we are not updating this flag if there are requests awaiting for responses
            // since their responses will reset this flag anyway.
            touched = false;
        }

        return idle;
    }

    public MessagingEndpoint<T> getEndpoint() {
        return endpoint;
    }

    protected void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    protected void touch() {
        // 1. Check if idling is enabled.
        // 2. Check if not touched yet (volatile read).
        if (trackIdleState && !touched) {
            // Update touched flag (it is ok if multiple threads would do it in parallel).
            touched = true;
        }
    }

    protected RequestHolder<T> registerRequest(AffinityWorker worker, T message, RequestCallback<T> callback) {
        while (true) {
            int id = idGen.incrementAndGet();

            RequestHolder<T> holder = new RequestHolder<>(id, worker, message, epoch, callback);

            // Do not overwrite very very very old requests.
            if (requests.putIfAbsent(id, holder) == null) {
                onRequestRegister();

                return holder;
            }
        }
    }

    protected void doReceiveRequest(Request<T> msg, AffinityWorker worker) {
        try {
            msg.prepareReceive(worker, this);

            receiver.receive(msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

            disconnectOnError(e);
        }
    }

    protected void doReceiveNotification(Notification<T> msg) {
        try {
            msg.prepareReceive(this);

            receiver.receive(msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

            disconnectOnError(e);
        }
    }

    protected void doReceiveResponse(Response<T> msg, RequestHolder<T> holder) {
        try {
            msg.prepareReceive(this, holder.getMessage());

            holder.getCallback().onComplete(null, msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

            disconnectOnError(e);
        }
    }

    protected void doReceiveConversation(Conversation<T> msg, RequestHolder<T> holder) {
        try {
            msg.prepareReceive(holder.getWorker(), this, holder.getMessage());

            holder.getCallback().onComplete(null, msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

            disconnectOnError(e);
        }
    }

    protected void doReceiveResponseChunk(ResponseChunk<T> msg, RequestHolder<T> holder) {
        try {
            msg.prepareReceive(this, holder.getMessage());

            holder.getCallback().onComplete(null, msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

            disconnectOnError(e);
        }
    }

    protected void onAsyncEnqueue() {
        if (metrics != null) {
            metrics.onAsyncEnqueue();
        }
    }

    protected void onAsyncDequeue() {
        if (metrics != null) {
            metrics.onAsyncDequeue();
        }
    }

    protected RequestHolder<T> unregisterRequest(int id) {
        RequestHolder<T> holder;

        holder = requests.remove(id);

        if (holder != null) {
            onRequestUnregister(1);
        }

        return holder;
    }

    protected RequestHolder<T> peekRequest(int id) {
        return requests.get(id);
    }

    private void onEnqueueReceiveAsync(NetworkEndpoint<MessagingProtocol> from) {
        onAsyncEnqueue();

        if (backPressure != null) {
            backPressure.onEnqueue(from);
        }
    }

    private void onDequeueReceiveAsync() {
        if (backPressure != null) {
            backPressure.onDequeue();
        }

        onAsyncDequeue();
    }

    private void doNotifyOnSendSuccess(T payload, SendCallback callback) {
        try {
            callback.onComplete(null);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);

            disconnectOnError(e);
        }
    }

    private void doNotifyOnReplyFailure(T payload, Throwable error, RequestCallback<T> callback) {
        try {
            callback.onComplete(error, null);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);
        } finally {
            disconnectOnError(error);
        }
    }

    private void doNotifyOnSendFailure(boolean disconnect, T payload, Throwable error, SendCallback callback) {
        try {
            callback.onComplete(error);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);
        } finally {
            if (disconnect) {
                disconnectOnError(error);
            }
        }
    }

    private void doDiscardRequest(Throwable cause, RequestHolder<T> holder) {
        T message = holder.getMessage();

        RequestCallback<T> callback = holder.getCallback();

        try {
            callback.onComplete(cause, null);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to notify callback on response failure [message={}]", message, e);
            }
        }
    }

    private void onRequestRegister() {
        if (metrics != null) {
            metrics.onPendingRequestAdded();
        }
    }

    private void onRequestUnregister(int amount) {
        if (metrics != null) {
            metrics.onPendingRequestsRemoved(amount);
        }
    }

    private void handleAsyncMessageError(Throwable error) {
        if (log.isErrorEnabled()) {
            log.error("Got error during message processing.", error);
        }

        disconnectOnError(error);
    }

    private int randomAffinity() {
        return ThreadLocalRandom.current().nextInt();
    }
}
