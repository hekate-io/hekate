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
import io.hekate.codec.CodecException;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.RequestForResponseBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidResponse;
import io.hekate.messaging.operation.SendCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

class MessagingConnectionIn<T> extends MessagingConnection<T> {
    private final Logger log;

    private final MessagingExecutor async;

    private final MessageReceiver<T> receiver;

    private final SendPressureGuard sendPressure;

    private final MessageInterceptors<T> interceptors;

    public MessagingConnectionIn(
        NetworkEndpoint<MessagingProtocol> net,
        MessagingEndpoint<T> endpoint,
        MessagingGatewayContext<T> gateway
    ) {
        super(gateway, endpoint, net);

        assert gateway.receiver() != null : "Receiver is not configured [channel=" + gateway + ']';

        this.log = gateway.log();
        this.async = gateway.async();
        this.sendPressure = gateway.sendGuard();
        this.interceptors = gateway.interceptors();
        this.receiver = gateway.receiver();
    }

    public NetworkFuture<MessagingProtocol> disconnect() {
        return network().disconnect();
    }

    public void onConnect() {
        receiver.onConnect(endpoint());
    }

    public void onDisconnect() {
        receiver.onDisconnect(endpoint());

        gateway().unregister(this);
    }

    public void replyChunk(MessagingWorker worker, T chunk, SubscribeRequest<T> request, SendCallback callback) {
        ResponseChunk<T> msg = new ResponseChunk<>(request.requestId(), chunk);

        // Prepare message.
        msg.prepareSend(this, request);

        // Apply interceptors.
        gateway().interceptors().serverSend(msg);

        // Try to apply back pressure.
        Throwable backPressureErr = null;

        if (sendPressure != null) {
            try {
                sendPressure.onEnqueue();
            } catch (InterruptedException | MessageQueueOverflowException e) {
                backPressureErr = e;
            }
        }

        if (backPressureErr == null) {
            // Send the message.
            network().send(msg, (sent, error) -> {
                if (sendPressure != null) {
                    sendPressure.onDequeue();
                }

                if (error == null) {
                    notifyResponseSendSuccess(worker, msg.payload(), callback);
                } else {
                    notifyResponseSendFailure(worker, msg.payload(), error, callback);
                }
            });
        } else {
            // Back pressure failure.
            notifyResponseSendFailure(worker, msg.payload(), backPressureErr, callback);
        }
    }

    public void replyFinal(MessagingWorker worker, T response, RequestForResponseBase<T> request, SendCallback callback) {
        FinalResponse<T> msg = new FinalResponse<>(request.requestId(), response);

        // Prepare message.
        msg.prepareSend(this, request);

        // Apply interceptors.
        gateway().interceptors().serverSend(msg);

        // Apply back pressure when sending from server back to client.
        if (sendPressure != null) {
            // Note we are using IGNORE policy for back pressure
            // since we don't want this operation to fail/block but still want it to be counted by the back pressure guard.
            sendPressure.onEnqueueIgnorePolicy();
        }

        network().send(msg, (sent, error) -> {
            if (sendPressure != null) {
                sendPressure.onDequeue();
            }

            if (error == null) {
                notifyResponseSendSuccess(worker, msg.payload(), callback);
            } else {
                if (error instanceof CodecException) {
                    replyError(msg.requestId(), error);
                }

                notifyResponseSendFailure(worker, msg.payload(), error, callback);
            }
        });
    }

    public void replyVoid(RequestBase<T> request) {
        network().send(new VoidResponse(request.requestId()));
    }

    public void replyError(int requestId, Throwable cause) {
        network().send(new ErrorResponse(requestId, ErrorUtils.stackTrace(cause)));
    }

    public void receive(NetworkMessage<MessagingProtocol> netMsg, NetworkEndpoint<MessagingProtocol> from) {
        try {
            MessagingProtocol.Type msgType = MessagingProtocolCodec.previewType(netMsg);

            switch (msgType) {
                case NOTIFICATION: {
                    MessagingWorker worker = async.pooledWorker();

                    if (worker.isAsync()) {
                        long receivedAtNanos = receivedAtNanos(netMsg);

                        onReceiveAsyncEnqueue(from);

                        netMsg.handleAsync(worker, msg -> {
                            onReceiveAsyncDequeue();

                            try {
                                receiveNotificationAsync(msg.decode().cast(), receivedAtNanos);
                            } catch (Throwable err) {
                                handleReceiveError(msg, err);
                            }
                        });
                    } else {
                        receiveNotificationSync(netMsg.decode().cast());
                    }

                    break;
                }
                case AFFINITY_NOTIFICATION: {
                    int affinity = MessagingProtocolCodec.previewAffinity(netMsg);
                    long receivedAtNanos = receivedAtNanos(netMsg);

                    MessagingWorker worker = async.workerFor(affinity);

                    if (worker.isAsync()) {
                        onReceiveAsyncEnqueue(from);

                        netMsg.handleAsync(worker, msg -> {
                            onReceiveAsyncDequeue();

                            try {
                                receiveNotificationAsync(msg.decode().cast(), receivedAtNanos);
                            } catch (Throwable err) {
                                handleReceiveError(msg, err);
                            }
                        });
                    } else {
                        receiveNotificationSync(netMsg.decode().cast());
                    }

                    break;
                }
                case REQUEST:
                case VOID_REQUEST:
                case SUBSCRIBE: {
                    MessagingWorker worker = async.pooledWorker();

                    if (worker.isAsync()) {
                        long receivedAtNanos = receivedAtNanos(netMsg);

                        onReceiveAsyncEnqueue(from);

                        netMsg.handleAsync(worker, msg -> {
                            onReceiveAsyncDequeue();

                            try {
                                receiveRequestAsync(msg.decode().cast(), worker, receivedAtNanos);
                            } catch (Throwable err) {
                                handleReceiveError(msg, err);
                            }
                        });
                    } else {
                        receiveRequestSync(netMsg.decode().cast(), worker);
                    }

                    break;
                }
                case AFFINITY_REQUEST:
                case AFFINITY_VOID_REQUEST:
                case AFFINITY_SUBSCRIBE: {
                    int affinity = MessagingProtocolCodec.previewAffinity(netMsg);

                    MessagingWorker worker = async.workerFor(affinity);

                    if (worker.isAsync()) {
                        long receivedAtNanos = receivedAtNanos(netMsg);

                        onReceiveAsyncEnqueue(from);

                        netMsg.handleAsync(worker, msg -> {
                            onReceiveAsyncDequeue();

                            try {
                                receiveRequestAsync(msg.decode().cast(), worker, receivedAtNanos);
                            } catch (Throwable err) {
                                handleReceiveError(msg, err);
                            }
                        });
                    } else {
                        receiveRequestSync(netMsg.decode().cast(), worker);
                    }

                    break;
                }
                case FINAL_RESPONSE:
                case RESPONSE_CHUNK:
                case VOID_RESPONSE:
                case ERROR_RESPONSE:
                case CONNECT:
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + msgType);
                }
            }
        } catch (Throwable t) {
            handleReceiveError(netMsg, t);
        }
    }

    private void receiveRequestSync(RequestBase<T> msg, MessagingWorker worker) {
        receiveRequestAsync(msg, worker, 0);
    }

    private void receiveRequestAsync(RequestBase<T> msg, MessagingWorker worker, long receivedAtNanos) {
        if (!isExpired(msg, receivedAtNanos)) {
            try {
                msg.prepareReceive(worker, this);

                interceptors.serverReceive(msg);

                try {
                    receiver.receive(msg);
                } finally {
                    interceptors.serverReceiveComplete(msg);
                }

                if (msg.isVoid()) {
                    replyVoid(msg);
                }
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during message processing [from={}, message={}]", msg.from(), msg, e);
                }

                replyError(msg.requestId(), e);
            }
        }
    }

    private void receiveNotificationSync(Notification<T> msg) {
        receiveNotificationAsync(msg, 0);
    }

    private void receiveNotificationAsync(Notification<T> msg, long receivedAtNanos) {
        if (!isExpired(msg, receivedAtNanos)) {
            try {
                msg.prepareReceive(this);

                interceptors.serverReceive(msg);

                try {
                    receiver.receive(msg);
                } finally {
                    interceptors.serverReceiveComplete(msg);
                }
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got an unexpected runtime error during message processing [from={}, message={}]", msg.from(), msg, e);
                }
            }
        }
    }

    private void handleReceiveError(NetworkMessage<MessagingProtocol> msg, Throwable err) {
        ClusterAddress from = endpoint().remoteAddress();

        if (err instanceof RequestPayloadDecodeException) {
            RequestPayloadDecodeException e = (RequestPayloadDecodeException)err;

            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("Failed to decode message [from={}]", from, cause);
            }

            replyError(e.requestId(), cause);
        } else if (err instanceof NotificationPayloadDecodeException) {
            log.error("Failed to decode message [from={}]", from, err);
        } else {
            log.error("Got an error during message processing [from={}, message={}]", from, msg, err);

            disconnect();
        }
    }

    private void notifyResponseSendSuccess(MessagingWorker worker, T msg, SendCallback callback) {
        if (callback != null) {
            if (worker.isAsync()) {
                worker.execute(() ->
                    doNotifyOnResponseSendSuccess(msg, callback)
                );
            } else {
                doNotifyOnResponseSendSuccess(msg, callback);
            }
        }
    }

    private void doNotifyOnResponseSendSuccess(T rsp, SendCallback callback) {
        try {
            callback.onComplete(null);
        } catch (RuntimeException | Error e) {
            log.error("Failed to notify on successful send of a response message [to={}, message={}]", remoteAddress(), rsp, e);
        }
    }

    private void notifyResponseSendFailure(MessagingWorker worker, T rsp, Throwable err, SendCallback callback) {
        if (err instanceof CodecException && log.isErrorEnabled()) {
            log.error("Failed to send message [to={}, message={}]", remoteAddress(), rsp, err);
        }

        if (callback != null) {
            if (worker.isAsync()) {
                worker.execute(() ->
                    doNotifyOnResponseSendFailure(rsp, err, callback)
                );
            } else {
                doNotifyOnResponseSendFailure(rsp, err, callback);
            }
        }
    }

    private void doNotifyOnResponseSendFailure(T rsp, Throwable err, SendCallback callback) {
        MessagingException msgError = wrapError(err);

        try {
            callback.onComplete(msgError);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to notify on response sending failure [to={}, failure={}, message={}]",
                    remoteAddress(), err.toString(), rsp, e);
            }
        }
    }

    private static boolean isExpired(RequestBase<?> msg, long receivedAtNanos) {
        return receivedAtNanos > 0 && System.nanoTime() - receivedAtNanos >= TimeUnit.MILLISECONDS.toNanos(msg.timeout());
    }

    private static boolean isExpired(Notification<?> msg, long receivedAtNanos) {
        return receivedAtNanos > 0 && System.nanoTime() - receivedAtNanos >= TimeUnit.MILLISECONDS.toNanos(msg.timeout());
    }

    private static long receivedAtNanos(NetworkMessage<MessagingProtocol> msg) throws IOException {
        if (MessagingProtocolCodec.previewHasTimeout(msg)) {
            return System.nanoTime();
        } else {
            return 0;
        }
    }
}
