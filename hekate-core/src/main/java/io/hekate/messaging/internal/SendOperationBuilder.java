package io.hekate.messaging.internal;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.messaging.unicast.Send;
import io.hekate.messaging.unicast.SendAckMode;
import io.hekate.messaging.unicast.SendFuture;

class SendOperationBuilder<T> extends MessageOperationBuilder<T> implements Send<T> {
    private Object affinity;

    private SendAckMode ackMode;

    public SendOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Send<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Send<T> withAckMode(SendAckMode ackMode) {
        ArgAssert.notNull(ackMode, "Acknowledgement mode");

        this.ackMode = ackMode;

        return this;
    }

    @Override
    public SendFuture submit() {
        SendOperation<T> op = new SendOperation<>(message(), affinity, gateway(), opts(), ackMode);

        gateway().submit(op);

        return op.future();
    }
}
