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
import io.hekate.codec.CodecException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.intercept.ServerReceiveContext;
import io.hekate.messaging.intercept.ServerSendContext;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.RequestForResponseBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

abstract class MessagingConnectionBase<T> {
    private final Logger log;

    private final MessagingGatewayContext<T> ctx;

    private final MessageReceiver<T> receiver;

    private final MessagingExecutor async;

    private final MessagingMetrics metrics;

    private final ReceivePressureGuard pressureGuard;

    private final MessagingEndpoint<T> endpoint;

    private final RequestRegistry<T> requests;

    public MessagingConnectionBase(MessagingGatewayContext<T> ctx, MessagingExecutor async, MessagingEndpoint<T> endpoint) {
        assert ctx != null : "Messaging context is null.";
        assert async != null : "Executor is null.";
        assert endpoint != null : "Messaging endpoint is null.";

        this.ctx = ctx;
        this.async = async;
        this.endpoint = endpoint;
        this.log = ctx.log();
        this.receiver = ctx.receiver();
        this.metrics = ctx.metrics();
        this.pressureGuard = ctx.receiveGuard();

        this.requests = new RequestRegistry<>(metrics);
    }

    public abstract NetworkFuture<MessagingProtocol> disconnect();

    public abstract void send(MessageAttempt<T> attempt, SendCallback callback);

    public abstract void request(MessageAttempt<T> attempt, InternalRequestCallback<T> callback);

    public abstract void replyChunk(MessagingWorker worker, T chunk, SubscribeRequest<T> request, SendCallback callback);

    public abstract void replyFinal(MessagingWorker worker, T response, RequestForResponseBase<T> request, SendCallback callback);

    public abstract void replyVoid(MessagingWorker worker, RequestBase<T> request);

    public abstract void replyError(MessagingWorker worker, int requestId, Throwable cause);

    protected abstract void disconnectOnError(Throwable t);

    protected abstract int epoch();

    public MessagingGatewayContext<T> gateway() {
        return ctx;
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
                        log.error("Received an unexpected notification [from={}, message={}]", remoteAddress(), netMsg);
                    } else {
                        if (async.isAsync()) {
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            MessagingWorker worker = async.pooledWorker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveNotificationAsync(m.cast(), receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveNotificationSync(netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case AFFINITY_NOTIFICATION: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected notification [from={}, message={}]", remoteAddress(), netMsg);
                        }
                    } else {
                        if (async.isAsync()) {
                            int affinity = MessagingProtocolCodec.previewAffinity(netMsg);
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            MessagingWorker worker = async.workerFor(affinity);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveNotificationAsync(m.cast(), receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveNotificationSync(netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case REQUEST:
                case VOID_REQUEST: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected request [from={}, message={}]", remoteAddress(), netMsg);
                        }
                    } else {
                        MessagingWorker worker = async.pooledWorker();

                        if (async.isAsync()) {
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveRequestAsync(m.cast(), worker, receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveRequestSync(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case AFFINITY_REQUEST:
                case AFFINITY_VOID_REQUEST: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected request [from={}, message={}]", remoteAddress(), netMsg);
                        }
                    } else {
                        int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                        MessagingWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveRequestAsync(m.cast(), worker, receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveRequestSync(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case SUBSCRIBE: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected subscription [from={}, message={}]", remoteAddress(), netMsg);
                        }
                    } else {
                        MessagingWorker worker = async.pooledWorker();

                        if (async.isAsync()) {
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveRequestAsync(m.cast(), worker, receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveRequestSync(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case AFFINITY_SUBSCRIBE: {
                    if (receiver == null) {
                        if (log.isErrorEnabled()) {
                            log.error("Received an unexpected subscription [from={}, message={}]", remoteAddress(), netMsg);
                        }
                    } else {
                        int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                        MessagingWorker worker = async.workerFor(affinity);

                        if (async.isAsync()) {
                            long receivedAtNanos = receivedAtNanos(netMsg);

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                receiveRequestAsync(m.cast(), worker, receivedAtNanos);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            receiveRequestSync(netMsg.decode().cast(), worker);
                        }
                    }

                    break;
                }
                case FINAL_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            MessagingWorker worker = handle.worker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveFinalResponse(handle, m.cast());
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            doReceiveFinalResponse(handle, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case RESPONSE_CHUNK: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            MessagingWorker worker = handle.worker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveResponseChunk(handle, m.cast());
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            doReceiveResponseChunk(handle, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case VOID_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            MessagingWorker worker = handle.worker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveVoidResponse(handle);
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            doReceiveVoidResponse(handle);
                        }
                    }

                    break;
                }
                case ERROR_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> handle = requests.get(requestId);

                    if (handle != null) {
                        if (async.isAsync()) {
                            MessagingWorker worker = handle.worker();

                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, m -> {
                                onReceiveAsyncDequeue();

                                doReceiveError(handle, m.cast());
                            }, error -> handleReceiveError(error, netMsg));
                        } else {
                            doReceiveError(handle, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case CONNECT: // Connect message is not expected here.
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + msgType);
                }
            }
        } catch (Throwable t) {
            handleReceiveError(t, netMsg);
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
        if (error instanceof CodecException && log.isErrorEnabled()) {
            log.error("Failed to send message [to={}, message={}]", remoteAddress(), payload, error);
        }

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

    public void notifyOnRequestFailure(RequestHandle<T> handle, Throwable err) {
        if (handle.unregister()) {
            if (async.isAsync()) {
                onAsyncEnqueue();

                handle.worker().execute(() -> {
                    onAsyncDequeue();

                    doNotifyOnRequestFailure(handle, err);
                });
            } else {
                doNotifyOnRequestFailure(handle, err);
            }
        }
    }

    public boolean hasPendingRequests() {
        return !requests.isEmpty();
    }

    public MessagingEndpoint<T> endpoint() {
        return endpoint;
    }

    public void discardRequests(Throwable cause) {
        discardRequests(epoch(), cause);
    }

    public void discardRequests(int epoch, Throwable cause) {
        List<RequestHandle<T>> discarded = requests.unregisterEpoch(epoch);

        for (RequestHandle<T> handle : discarded) {
            MessagingWorker worker = handle.worker();

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

    public T interceptServerReceive(T msg, ServerReceiveContext inCtx) {
        return ctx.intercept().serverReceive(msg, inCtx);
    }

    public T interceptServerSend(T msg, ServerSendContext rspCtx, ServerReceiveContext rcvCtx) {
        return ctx.intercept().serverSend(msg, rspCtx, rcvCtx);
    }

    protected RequestHandle<T> registerRequest(MessageAttempt<T> attempt, InternalRequestCallback<T> callback) {
        return requests.register(epoch(), attempt, callback);
    }

    protected void receiveRequestAsync(RequestBase<T> msg, MessagingWorker worker, long receivedAtNanos) {
        if (!isExpired(msg, receivedAtNanos)) {
            try {
                msg.prepareReceive(worker, this);

                try {
                    receiver.receive(msg);
                } finally {
                    ctx.intercept().serverReceiveComplete(msg.get(), msg);
                }

                if (msg.isVoid()) {
                    replyVoid(worker, msg);
                }
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during request processing [from={}, message={}]", msg.from(), msg, e);
                }

                replyError(worker, msg.requestId(), e);
            }
        }
    }

    protected void receiveNotificationAsync(Notification<T> msg, long receivedAtNanos) {
        if (!isExpired(msg, receivedAtNanos)) {
            try {
                msg.prepareReceive(this);

                try {
                    receiver.receive(msg);
                } finally {
                    ctx.intercept().serverReceiveComplete(msg.get(), msg);
                }
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during notification processing [from={}, message={}]", msg.from(), msg, e);
                }
            }
        }
    }

    protected void doReceiveFinalResponse(RequestHandle<T> request, FinalResponse<T> msg) {
        if (request.isRegistered()) {
            try {
                msg.prepareReceive(this, request.attempt());

                request.callback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during response processing [from={}, message={}]", msg.from(), msg, e);
                }
            }
        }
    }

    protected void doReceiveVoidResponse(RequestHandle<T> request) {
        if (request.isRegistered()) {
            try {
                request.attempt().interceptReceiveVoid();

                request.callback().onComplete(request, null, null);
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during acknowledgement processing [from={}, message={}]",
                        remoteAddress(), request.context().originalMessage(), e);
                }
            }
        }
    }

    protected void doReceiveResponseChunk(RequestHandle<T> request, ResponseChunk<T> msg) {
        if (request.isRegistered()) {
            try {
                msg.prepareReceive(this, request.attempt());

                request.callback().onComplete(request, null, msg);
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during response chunk processing [from={}, message={}]", msg.from(), msg, e);
                }

                if (request.unregister()) {
                    doNotifyOnRequestFailure(request, e);
                }
            }
        }
    }

    protected void doReceiveError(RequestHandle<T> handle, ErrorResponse response) {
        MessagingRemoteException error = new MessagingRemoteException("Request processing failed on remote node "
            + "[node=" + remoteAddress() + "]", response.stackTrace());

        notifyOnRequestFailure(handle, error);
    }

    protected void onAsyncEnqueue() {
        metrics.onAsyncEnqueue();
    }

    protected void onAsyncDequeue() {
        metrics.onAsyncDequeue();
    }

    protected ClusterAddress remoteAddress() {
        return endpoint().remoteAddress();
    }

    private void receiveRequestSync(RequestBase<T> msg, MessagingWorker worker) {
        receiveRequestAsync(msg, worker, 0);
    }

    private void receiveNotificationSync(Notification<T> msg) {
        receiveNotificationAsync(msg, 0);
    }

    private boolean isExpired(RequestBase<T> msg, long receivedAtNanos) {
        return receivedAtNanos > 0 && System.nanoTime() - receivedAtNanos >= TimeUnit.MILLISECONDS.toNanos(msg.timeout());
    }

    private boolean isExpired(Notification<T> msg, long receivedAtNanos) {
        return receivedAtNanos > 0 && System.nanoTime() - receivedAtNanos >= TimeUnit.MILLISECONDS.toNanos(msg.timeout());
    }

    private void handleReceiveError(Throwable error, NetworkMessage<MessagingProtocol> msg) {
        ClusterAddress from = endpoint.remoteAddress();

        if (error instanceof RequestPayloadDecodeException) {
            RequestPayloadDecodeException e = (RequestPayloadDecodeException)error;

            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("Failed to decode request message [from={}]", from, cause);
            }

            MessagingWorker worker;

            if (e.affinity().isPresent()) {
                worker = async.workerFor(e.affinity().getAsInt());
            } else {
                worker = async.pooledWorker();
            }

            replyError(worker, e.requestId(), cause);
        } else if (error instanceof ResponsePayloadDecodeException) {
            ResponsePayloadDecodeException e = (ResponsePayloadDecodeException)error;

            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("Failed to decode response message [from={}]", from, cause);
            }

            RequestHandle<T> handle = requests.get(e.requestId());

            if (handle != null) {
                notifyOnRequestFailure(handle, cause);
            }
        } else if (error instanceof NotificationPayloadDecodeException) {
            log.error("Failed to decode notification message [from={}]", from, error);
        } else {
            log.error("Got error during message processing [from={}, message={}]", from, msg, error);

            disconnectOnError(error);
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
            MessagingException msgError;

            if (error instanceof MessagingException) {
                msgError = (MessagingException)error;
            } else {
                msgError = new MessagingException("Message send failure [node=" + remoteAddress() + ']', error);
            }

            callback.onComplete(msgError);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during message processing [message={}]", payload, e);
            }
        }
    }

    private void doNotifyOnRequestFailure(RequestHandle<T> handle, Throwable error) {
        try {
            MessagingException msgError;

            if (error instanceof MessagingException) {
                msgError = (MessagingException)error;
            } else {
                msgError = new MessagingException("Messaging request failure [node=" + remoteAddress() + ']', error);
            }

            handle.attempt().interceptReceiveError(msgError);

            handle.callback().onComplete(handle, msgError, null);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during message processing [message={}]", handle.message(), e);
            }
        }
    }

    private void doDiscardRequest(Throwable cause, RequestHandle<T> handle) {
        T message = handle.message();

        try {
            handle.callback().onComplete(handle, cause, null);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to notify callback on response failure [message={}]", message, e);
            }
        }
    }

    private long receivedAtNanos(NetworkMessage<MessagingProtocol> netMsg) throws IOException {
        return MessagingProtocolCodec.previewHasTimeout(netMsg) ? System.nanoTime() : 0;
    }
}
