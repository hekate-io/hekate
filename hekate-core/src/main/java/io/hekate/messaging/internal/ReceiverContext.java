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
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.Response;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class ReceiverContext<T> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final MessagingGateway<T> gateway;

    private final MessageReceiver<T> receiver;

    private final AffinityExecutor async;

    private final MetricsCallback metrics;

    private final ReceiveBackPressure backPressure;

    private final MessagingEndpoint<T> endpoint;

    private final boolean trackIdleState;

    private final RequestRegistry<T> requests;

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

        this.requests = new RequestRegistry<>(metrics);
    }

    public abstract NetworkFuture<MessagingProtocol> disconnect();

    public abstract void sendNotification(MessageContext<T> ctx, SendCallback callback);

    public abstract void sendRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback);

    public abstract void replyChunk(AffinityWorker worker, int requestId, T chunk, SendCallback callback);

    public abstract void reply(AffinityWorker worker, int requestId, T response, SendCallback callback);

    protected abstract void disconnectOnError(Throwable t);

    public MessagingGateway<T> gateway() {
        return gateway;
    }

    public RequestRegistry<T> requests() {
        return requests;
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

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            AffinityWorker worker = handle.getWorker();

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveResponse(handle, m.cast());
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveResponse(handle, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case RESPONSE_CHUNK: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            AffinityWorker worker = handle.getWorker();

                            onEnqueueReceiveAsync(from);

                            netMsg.handleAsync(worker, m -> {
                                onDequeueReceiveAsync();

                                doReceiveResponseChunk(handle, m.cast());
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveResponseChunk(handle, netMsg.decode().cast());
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

    public void notifyOnSendFailure(AffinityWorker worker, T payload, Throwable error, SendCallback callback) {
        if (callback != null) {
            if (async.isAsync()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnSendFailure(payload, error, callback);
                });
            } else {
                doNotifyOnSendFailure(payload, error, callback);
            }
        }
    }

    public void notifyOnReplyFailure(RequestHandle<T> handle, Throwable err) {
        if (handle.unregister()) {
            if (async.isAsync()) {
                onAsyncEnqueue();

                handle.getWorker().execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnReplyFailure(handle, err);
                });
            } else {
                doNotifyOnReplyFailure(handle, err);
            }
        }
    }

    public void discardRequests(Throwable cause) {
        discardRequests(epoch, cause);
    }

    public void discardRequests(int epoch, Throwable cause) {
        List<RequestHandle<T>> discarded = requests.unregisterEpoch(epoch);

        for (RequestHandle<T> handle : discarded) {
            AffinityWorker worker = handle.getWorker();

            if (async.isAsync()) {
                onAsyncEnqueue();

                worker.execute(() -> {
                    onAsyncDequeue();

                    doDiscardRequest(cause, handle);
                });
            } else {
                doDiscardRequest(cause, handle);
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

    protected RequestHandle<T> registerRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        return requests.register(epoch, ctx, callback);
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

    protected void doReceiveResponse(RequestHandle<T> request, Response<T> msg) {
        if (request.isRegistered()) {
            try {
                msg.prepareReceive(this, request.getMessage());

                request.getCallback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

                disconnectOnError(e);
            }
        }
    }

    protected void doReceiveResponseChunk(RequestHandle<T> request, ResponseChunk<T> msg) {
        if (request.isRegistered()) {
            try {
                if (request.getContext().getOpts().hasTimeout()) {
                    // Reset timeout on every response chunk.
                    gateway().scheduleTimeout(request.getContext(), request.getCallback());
                }

                msg.prepareReceive(this, request.getMessage());

                request.getCallback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error during message processing [message={}]", msg, e);

                disconnectOnError(e);
            }
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

    private void doNotifyOnSendFailure(T payload, Throwable error, SendCallback callback) {
        try {
            callback.onComplete(error);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);
        }
    }

    private void doNotifyOnReplyFailure(RequestHandle<T> handle, Throwable error) {
        try {
            handle.getCallback().onComplete(handle, error, null);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", handle.getMessage(), e);
        }
    }

    private void doDiscardRequest(Throwable cause, RequestHandle<T> handle) {
        T message = handle.getMessage();

        try {
            handle.getCallback().onComplete(handle, cause, null);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to notify callback on response failure [message={}]", message, e);
            }
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
