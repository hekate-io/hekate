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
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
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

abstract class MessagingConnectionBase<T> {
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final MessagingGateway<T> gateway;

    private final MessageReceiver<T> receiver;

    private final MessagingExecutor async;

    private final MetricsCallback metrics;

    private final ReceivePressureGuard pressureGuard;

    private final MessagingEndpoint<T> endpoint;

    private final RequestRegistry<T> requests;

    private volatile int epoch;

    public MessagingConnectionBase(MessagingGateway<T> gateway, MessagingExecutor async, MessagingEndpoint<T> endpoint) {
        assert gateway != null : "Gateway is null.";
        assert async != null : "Executor is null.";
        assert endpoint != null : "Messaging endpoint is null.";

        this.gateway = gateway;
        this.async = async;
        this.receiver = gateway.getReceiver();
        this.endpoint = endpoint;
        this.metrics = gateway.getMetrics();
        this.pressureGuard = gateway.getReceivePressureGuard();

        this.requests = new RequestRegistry<>(metrics);
    }

    public abstract NetworkFuture<MessagingProtocol> disconnect();

    public abstract void sendNotification(MessageContext<T> ctx, SendCallback callback);

    public abstract void sendSingleRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback);

    public abstract void sendStreamRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback);

    public abstract void replyChunk(MessagingWorker worker, int requestId, T chunk, SendCallback callback);

    public abstract void reply(MessagingWorker worker, int requestId, T response, SendCallback callback);

    protected abstract void disconnectOnError(Throwable t);

    public MessagingGateway<T> gateway() {
        return gateway;
    }

    public RequestRegistry<T> requests() {
        return requests;
    }

    public void receive(NetworkMessage<MessagingProtocol> netMsg, NetworkEndpoint<MessagingProtocol> from) {
        try {
            MessagingProtocol.Type msgType = MessagingProtocolCodec.previewType(netMsg);

            switch (msgType) {
                case NOTIFICATION: {
                    if (receiver == null) {
                        log.error("Received an unexpected message [message={}]", netMsg);
                    } else {
                        if (async.isAsync()) {
                            int affinity = randomAffinity();

                            MessagingWorker worker = async.workerFor(affinity);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

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
                        log.error("Received an unexpected message [message={}, from={}]", netMsg, from);
                    } else {
                        if (async.isAsync()) {
                            int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                            MessagingWorker worker = async.workerFor(affinity);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

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
                        log.error("Received an unexpected message [message={}, from={}]", netMsg, from);
                    } else {
                        MessagingWorker worker = async.pooledWorker();

                        if (async.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

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
                        log.error("Received an unexpected message [message={}, from={}]", netMsg, from);
                    } else {
                        int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                        MessagingWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveRequest(m.cast(), worker);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveRequest(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case STREAM_REQUEST: {
                    if (receiver == null) {
                        log.error("Received an unexpected message [message={}, from={}]", netMsg, from);
                    } else {
                        // Use artificial affinity since all of the stream's operations must be handled by the same stream.
                        int affinity = randomAffinity();

                        MessagingWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveRequest(m.cast(), worker);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveRequest(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case AFFINITY_STREAM_REQUEST: {
                    if (receiver == null) {
                        log.error("Received an unexpected message [message={}, from={}]", netMsg, from);
                    } else {
                        int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                        MessagingWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveRequest(m.cast(), worker);
                            }, this::handleAsyncMessageError);
                        } else {
                            doReceiveRequest(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case FINAL_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            MessagingWorker worker = handle.getWorker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveResponse(handle, m.cast());
                            }, error -> notifyOnReplyFailure(handle, error));
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
                            MessagingWorker worker = handle.getWorker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveResponseChunk(handle, m.cast());
                            }, error -> notifyOnReplyFailure(handle, error));
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
            log.error("Failed to decode network message [message={}], from={}]", netMsg, from, e);

            disconnectOnError(e);
        }
    }

    public void notifyOnSendSuccess(MessagingWorker worker, T payload, SendCallback callback) {
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

    public void notifyOnSendFailure(MessagingWorker worker, T payload, Throwable error, SendCallback callback) {
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

    public boolean hasPendingRequests() {
        return !requests.isEmpty();
    }

    public MessagingEndpoint<T> getEndpoint() {
        return endpoint;
    }

    public void discardRequests(Throwable cause) {
        discardRequests(epoch, cause);
    }

    public void discardRequests(int epoch, Throwable cause) {
        List<RequestHandle<T>> discarded = requests.unregisterEpoch(epoch);

        for (RequestHandle<T> handle : discarded) {
            MessagingWorker worker = handle.getWorker();

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

    protected RequestHandle<T> registerRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        return requests.register(epoch, ctx, callback);
    }

    protected void setEpoch(int epoch) {
        this.epoch = epoch;
    }

    protected void doReceiveRequest(RequestBase<T> msg, MessagingWorker worker) {
        try {
            msg.prepareReceive(worker, this);

            receiver.receive(msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during request processing [message={}]", msg, e);

            // TODO: Send back an error response instead of closing the connection?
            disconnectOnError(e);
        }
    }

    protected void doReceiveNotification(Notification<T> msg) {
        try {
            msg.prepareReceive(this);

            receiver.receive(msg);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during notification processing [message={}]", msg, e);
        }
    }

    protected void doReceiveResponse(RequestHandle<T> request, FinalResponse<T> msg) {
        if (request.isRegistered()) {
            try {
                msg.prepareReceive(this, request.getMessage());

                request.getCallback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error during response processing [message={}]", msg, e);
            }
        }
    }

    protected void doReceiveResponseChunk(RequestHandle<T> request, ResponseChunk<T> msg) {
        if (request.isRegistered()) {
            if (request.getContext().opts().hasTimeout()) {
                // Reset timeout on every response chunk.
                gateway().scheduleTimeout(request.getContext(), request.getCallback());
            }

            try {
                msg.prepareReceive(this, request.getMessage());

                request.getCallback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error during response chunk processing [message={}]", msg, e);

                if (request.unregister()) {
                    doNotifyOnReplyFailure(request, e);
                }
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

    private void onReceiveAsyncEnqueue(NetworkEndpoint<MessagingProtocol> from) {
        onAsyncEnqueue();

        if (pressureGuard != null) {
            pressureGuard.onEnqueue(from);
        }
    }

    private void onReceiveAsyncDequeue() {
        if (pressureGuard != null) {
            pressureGuard.onDequeue();
        }

        onAsyncDequeue();
    }

    private void doNotifyOnSendSuccess(T payload, SendCallback callback) {
        try {
            callback.onComplete(null);
        } catch (RuntimeException | Error e) {
            log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);
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
            log.error("Failed to notify callback on response failure [message={}]", message, e);
        }
    }

    private void handleAsyncMessageError(Throwable error) {
        log.error("Got error during message processing.", error);
    }

    private int randomAffinity() {
        return ThreadLocalRandom.current().nextInt();
    }
}
