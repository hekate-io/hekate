package io.hekate.messaging.internal;

import io.hekate.messaging.unicast.Send;
import io.hekate.messaging.unicast.SendFuture;

class SendOperationBuilder<T> extends MessageOperationBuilder<T> implements Send<T> {
    private Object affinity;

    private boolean confirmReceive;

    public SendOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        super(message, gateway, opts);
    }

    @Override
    public Send<T> withAffinity(Object affinity) {
        this.affinity = affinity;

        return this;
    }

    @Override
    public Send<T> withConfirmReceive(boolean confirmReceive) {
        this.confirmReceive = confirmReceive;

        return this;
    }

    @Override
    public SendFuture submit() {
        SendOperation<T> op = new SendOperation<>(message(), affinity, gateway(), opts(), confirmReceive);

        gateway().submit(op);

        return op.future();
    }
}
