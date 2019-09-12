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
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.messaging.internal.MessagingProtocol.ErrorResponse;
import io.hekate.messaging.internal.MessagingProtocol.FinalResponse;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkSendCallback;
import java.nio.channels.ClosedChannelException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.slf4j.Logger;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

class MessagingConnectionOut<T> extends MessagingConnection<T> {
    interface DisconnectCallback {
        void onDisconnect();
    }

    private static final AtomicIntegerFieldUpdater<MessagingConnectionOut> EPOCH_UPDATER = newUpdater(
        MessagingConnectionOut.class,
        "connectEpoch"
    );

    private final Logger log;

    private final Object mux;

    private final DisconnectCallback callback;

    private final RequestRegistry<T> requests;

    private final NetworkClient<MessagingProtocol> net;

    @SuppressWarnings("unused") // <-- Updated via AtomicIntegerFieldUpdater.
    private volatile int connectEpoch;

    public MessagingConnectionOut(
        NetworkClient<MessagingProtocol> net,
        MessagingGatewayContext<T> gateway,
        MessagingEndpoint<T> endpoint,
        Object mux,
        DisconnectCallback callback
    ) {
        super(gateway, endpoint, net);

        this.net = net;
        this.mux = mux;
        this.log = gateway.log();
        this.callback = callback;

        this.requests = new RequestRegistry<>(gateway.metrics());
    }

    public NetworkFuture<MessagingProtocol> connect() {
        Connect payload = new Connect(
            remoteAddress().id(),
            gateway().localNode().address(),
            gateway().channelId()
        );

        synchronized (mux) {
            // Update the connection's epoch.
            int localEpoch = EPOCH_UPDATER.incrementAndGet(this);

            return net.connect(remoteAddress().socket(), payload, new NetworkClientCallback<MessagingProtocol>() {
                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> message, NetworkClient<MessagingProtocol> from) {
                    receive(message, from);
                }

                @Override
                public void onDisconnect(NetworkClient<MessagingProtocol> client, Optional<Throwable> cause) {
                    // Notify only if this is an internal disconnect.
                    synchronized (mux) {
                        if (localEpoch == connectEpoch) {
                            callback.onDisconnect();
                        }
                    }

                    discardRequestsWithError(localEpoch, wrapError(cause.orElseGet(ClosedChannelException::new)));
                }
            });
        }
    }

    public void send(MessagingProtocol msg, NetworkSendCallback<MessagingProtocol> callback) {
        net.send(msg, callback);
    }

    public NetworkFuture<MessagingProtocol> disconnect() {
        synchronized (mux) {
            return net.disconnect();
        }
    }

    public NetworkClient.State state() {
        return net.state();
    }

    public boolean hasPendingRequests() {
        return !requests.isEmpty();
    }

    public RequestHandle<T> registerRequest(MessageOperationAttempt<T> attempt) {
        return requests.register(connectEpoch, attempt);
    }

    public void discardRequestsWithError(int epoch, Throwable cause) {
        List<RequestHandle<T>> discarded = requests.unregisterEpoch(epoch);

        for (RequestHandle<T> req : discarded) {
            MessagingWorker worker = req.worker();

            if (worker.isAsync()) {
                worker.execute(() ->
                    doNotifyOnRequestFailure(req, cause)
                );
            } else {
                doNotifyOnRequestFailure(req, cause);
            }
        }
    }

    public void receive(NetworkMessage<MessagingProtocol> netMsg, NetworkEndpoint<MessagingProtocol> from) {
        try {
            MessagingProtocol.Type msgType = MessagingProtocolCodec.previewType(netMsg);

            switch (msgType) {
                case FINAL_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> req = requests.get(requestId);

                    if (req != null) {
                        MessagingWorker worker = req.worker();

                        if (worker.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, msg -> {
                                onReceiveAsyncDequeue();

                                try {
                                    doReceiveFinalResponse(req, msg.decode().cast());
                                } catch (Throwable err) {
                                    handleReceiveError(msg, err);
                                }
                            });
                        } else {
                            doReceiveFinalResponse(req, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case RESPONSE_CHUNK: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> req = requests.get(requestId);

                    if (req != null) {
                        MessagingWorker worker = req.worker();

                        if (worker.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, msg -> {
                                onReceiveAsyncDequeue();

                                try {
                                    doReceiveResponseChunk(req, msg.decode().cast());
                                } catch (Throwable err) {
                                    handleReceiveError(msg, err);
                                }
                            });
                        } else {
                            doReceiveResponseChunk(req, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case VOID_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> req = requests.get(requestId);

                    if (req != null) {
                        MessagingWorker worker = req.worker();

                        if (worker.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, msg -> {
                                onReceiveAsyncDequeue();

                                try {
                                    doReceiveVoidResponse(req);
                                } catch (Throwable err) {
                                    handleReceiveError(msg, err);
                                }
                            });
                        } else {
                            doReceiveVoidResponse(req);
                        }
                    }

                    break;
                }
                case ERROR_RESPONSE: {
                    int requestId = MessagingProtocolCodec.previewRequestId(netMsg);

                    RequestHandle<T> req = requests.get(requestId);

                    if (req != null) {
                        MessagingWorker worker = req.worker();

                        if (worker.isAsync()) {
                            onReceiveAsyncEnqueue(from);

                            netMsg.handleAsync(worker, msg -> {
                                onReceiveAsyncDequeue();

                                try {
                                    doReceiveRemoteError(req, msg.decode().cast());
                                } catch (Throwable err) {
                                    handleReceiveError(msg, err);
                                }
                            });
                        } else {
                            doReceiveRemoteError(req, netMsg.decode().cast());
                        }
                    }

                    break;
                }
                case NOTIFICATION:
                case AFFINITY_NOTIFICATION:
                case REQUEST:
                case VOID_REQUEST:
                case AFFINITY_REQUEST:
                case AFFINITY_VOID_REQUEST:
                case SUBSCRIBE:
                case AFFINITY_SUBSCRIBE:
                case CONNECT:
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + msgType);
                }
            }
        } catch (Throwable t) {
            handleReceiveError(netMsg, t);
        }
    }

    private void doReceiveFinalResponse(RequestHandle<T> req, FinalResponse<T> msg) {
        try {
            msg.prepareReceive(this, req.attempt());

            req.attempt().receive(msg);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during response processing [from={}, message={}]", msg.from(), msg, e);
            }
        }
    }

    private void doReceiveResponseChunk(RequestHandle<T> req, ResponseChunk<T> msg) {
        try {
            msg.prepareReceive(this, req.attempt());

            req.attempt().receive(msg);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Got an unexpected runtime error during response chunk processing [from={}, message={}]", msg.from(), msg, e);
            }

            doNotifyOnRequestFailure(req, e);
        }
    }

    private void doReceiveVoidResponse(RequestHandle<T> req) {
        try {
            req.attempt().receive(null);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                T msg = req.attempt().operation().message();

                log.error("Got an unexpected runtime error during confirmation processing [from={}, message={}]", remoteAddress(), msg, e);
            }
        }
    }

    private void doReceiveRemoteError(RequestHandle<T> req, ErrorResponse response) {
        String errMsg = "Request processing failed on remote node [node=" + remoteAddress() + "]";

        MessagingRemoteException err = new MessagingRemoteException(errMsg, response.stackTrace());

        notifyOnRequestFailure(req, err);
    }

    private void notifyOnRequestFailure(RequestHandle<T> req, Throwable err) {
        MessagingWorker worker = req.worker();

        if (worker.isAsync()) {
            worker.execute(() ->
                doNotifyOnRequestFailure(req, err)
            );
        } else {
            doNotifyOnRequestFailure(req, err);
        }
    }

    private void doNotifyOnRequestFailure(RequestHandle<T> req, Throwable err) {
        MessagingException msgError = wrapError(err);

        try {
            req.attempt().fail(msgError);
        } catch (RuntimeException | Error e) {
            if (log.isErrorEnabled()) {
                log.error("Failed to notify on messaging failure [to={}, failure={}, message={}]",
                    remoteAddress(), err.toString(), req.message(), e);
            }
        }
    }

    private void handleReceiveError(NetworkMessage<MessagingProtocol> msg, Throwable err) {
        ClusterAddress from = endpoint().remoteAddress();

        if (err instanceof ResponsePayloadDecodeException) {
            ResponsePayloadDecodeException e = (ResponsePayloadDecodeException)err;

            Throwable cause = e.getCause();

            if (log.isErrorEnabled()) {
                log.error("Failed to decode response message [from={}]", from, cause);
            }

            RequestHandle<T> req = requests.get(e.requestId());

            if (req != null) {
                notifyOnRequestFailure(req, cause);
            }
        } else {
            log.error("Got error during message processing [from={}, message={}]", from, msg, err);

            disconnect();
        }
    }
}
