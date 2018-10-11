package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.intercept.ClientSendContext;
import io.hekate.messaging.intercept.OutboundType;
import io.hekate.messaging.internal.MessagingProtocol.AffinityNotification;
import io.hekate.messaging.internal.MessagingProtocol.AffinityRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinitySubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.AffinityVoidRequest;
import io.hekate.messaging.internal.MessagingProtocol.Notification;
import io.hekate.messaging.internal.MessagingProtocol.Request;
import io.hekate.messaging.internal.MessagingProtocol.RequestBase;
import io.hekate.messaging.internal.MessagingProtocol.ResponseChunk;
import io.hekate.messaging.internal.MessagingProtocol.SubscribeRequest;
import io.hekate.messaging.internal.MessagingProtocol.VoidRequest;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class MessageOperationAttempt<T> implements ClientSendContext<T> {
    private final MessagingClient<T> client;

    private final ClusterTopology topology;

    private final MessageOperation<T> operation;

    private final Optional<FailureInfo> prevFailure;

    private final MessageOperationCallback<T> callback;

    private T payload;

    private MessageMetaData metaData;

    private Map<String, Object> attributes;

    private RequestHandle<T> reqHandle;

    public MessageOperationAttempt(
        MessagingClient<T> client,
        ClusterTopology topology,
        MessageOperation<T> operation,
        Optional<FailureInfo> prevFailure,
        MessageOperationCallback<T> callback
    ) {
        this(client, topology, operation, prevFailure, callback, null, null);
    }

    private MessageOperationAttempt(
        MessagingClient<T> client,
        ClusterTopology topology,
        MessageOperation<T> operation,
        Optional<FailureInfo> prevFailure,
        MessageOperationCallback<T> callback,
        MessageMetaData metaData,
        Map<String, Object> attributes
    ) {
        this.client = client;
        this.topology = topology;
        this.operation = operation;
        this.prevFailure = prevFailure;
        this.callback = callback;
        this.metaData = metaData;
        this.attributes = attributes;

        this.payload = operation.message();
    }

    public MessageOperationAttempt<T> nextAttempt(Optional<FailureInfo> failure) {
        return new MessageOperationAttempt<>(client, topology, operation, failure, callback, metaData, attributes);
    }

    public void submit() {
        // Apply interceptors.
        operation.gateway().interceptors().clientSend(this);

        // Build and submit the message.
        long timeout = operation.opts().timeout();
        boolean isRetransmit = prevFailure.isPresent();
        MessageMetaData metaData = hasMetaData() ? metaData() : null;

        MessagingConnectionOut<T> conn = client.connection();

        switch (type()) {
            case REQUEST: {
                reqHandle = conn.registerRequest(this);

                RequestBase<T> req;

                if (operation.hasAffinity()) {
                    req = new AffinityRequest<>(
                        operation.affinity(),
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                } else {
                    req = new Request<>(
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                }

                doSubmit(req, conn);

                break;
            }
            case SUBSCRIBE: {
                reqHandle = conn.registerRequest(this);

                RequestBase<T> req;

                if (operation.hasAffinity()) {
                    req = new AffinitySubscribeRequest<>(
                        operation.affinity(),
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                } else {
                    req = new SubscribeRequest<>(
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                }

                doSubmit(req, conn);

                break;
            }
            case SEND_WITH_ACK: {
                reqHandle = conn.registerRequest(this);

                RequestBase<T> req;

                if (operation.hasAffinity()) {
                    req = new AffinityVoidRequest<>(
                        operation.affinity(),
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                } else {
                    req = new VoidRequest<>(
                        reqHandle.id(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                }

                doSubmit(req, conn);

                break;
            }
            case SEND_NO_ACK: {
                Notification<T> msg;

                if (operation.hasAffinity()) {
                    msg = new AffinityNotification<>(
                        operation.affinity(),
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                } else {
                    msg = new Notification<>(
                        isRetransmit,
                        timeout,
                        payload,
                        metaData
                    );
                }

                doSubmit(msg, conn);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unsupported message type: " + type());
            }
        }
    }

    public void receive(ResponseChunk<T> rsp) {
        // TODO: Catch all errors.
        if (rsp == null) {
            if (operation.type() == OutboundType.SEND_WITH_ACK) {
                operation.gateway().interceptors().clientReceiveConfirmation(this);
            }
        } else {
            operation.gateway().interceptors().clientReceive(rsp);
        }

        if (callback.completeAttempt(this, rsp, null)) {
            if (reqHandle != null) {
                reqHandle.unregister();
            }
        }
    }

    public void fail(Throwable err) {
        // TODO: Catch all errors.
        operation.gateway().interceptors().clientReceiveError(err, this);

        if (callback.completeAttempt(this, null, err)) {
            if (reqHandle != null) {
                reqHandle.unregister();
            }
        }
    }

    public MessagingClient<T> client() {
        return client;
    }

    public MessageOperation<T> operation() {
        return operation;
    }

    @Override
    public OutboundType type() {
        return operation.type();
    }

    @Override
    public T get() {
        return operation.message();
    }

    @Override
    public String channelName() {
        return operation.gateway().name();
    }

    @Override
    public MessageMetaData metaData() {
        if (metaData == null) {
            metaData = new MessageMetaData();
        }

        return metaData;
    }

    @Override
    public void overrideMessage(T msg) {
        ArgAssert.notNull(msg, "Message");

        this.payload = msg;
    }

    @Override
    public boolean hasMetaData() {
        return metaData != null;
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
    public ClusterNode receiver() {
        return client.node();
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    @Override
    public boolean hasAffinity() {
        return operation.hasAffinity();
    }

    @Override
    public int affinity() {
        return operation.affinity();
    }

    @Override
    public Object affinityKey() {
        return operation.affinityKey();
    }

    @Override
    public Optional<FailureInfo> prevFailure() {
        return prevFailure;
    }

    private void doSubmit(Notification<T> msg, MessagingConnectionOut<T> conn) {
        msg.prepareSend(conn);

        conn.send(msg, (ignore, err) -> {
            if (err == null) {
                MessagingWorker worker = operation.worker();

                if (worker.isAsync()) {
                    worker.execute(() ->
                        receive(null)
                    );
                } else {
                    receive(null);
                }
            } else {
                failAsync(conn, err);
            }
        });
    }

    private void doSubmit(RequestBase<T> req, MessagingConnectionOut<T> conn) {
        req.prepareSend(operation.worker(), conn);

        conn.network().send(req, (msg, err) -> {
            if (err != null) {
                failAsync(conn, err);
            }
        });
    }

    private void failAsync(MessagingConnectionOut<T> conn, Throwable err) {
        MessagingWorker worker = operation.worker();

        if (worker.isAsync()) {
            worker.execute(() ->
                fail(wrapError(conn, err))
            );
        } else {
            fail(wrapError(conn, err));
        }
    }

    private MessagingException wrapError(MessagingConnectionOut<T> conn, Throwable err) {
        if (err instanceof MessagingException) {
            return (MessagingException)err;
        } else {
            return new MessagingException("Messaging operation failure [node=" + conn.remoteAddress() + ']', err);
        }
    }
}
