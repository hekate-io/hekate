package io.hekate.messaging.internal;

abstract class MessageOperationBuilder<T> {
    private final T message;

    private final MessageOperationOpts<T> opts;

    private final MessagingGatewayContext<T> gateway;

    public MessageOperationBuilder(T message, MessagingGatewayContext<T> gateway, MessageOperationOpts<T> opts) {
        this.message = message;
        this.opts = opts;
        this.gateway = gateway;
    }

    public MessageOperationOpts<T> opts() {
        return opts;
    }

    public T message() {
        return message;
    }

    public MessagingGatewayContext<T> gateway() {
        return gateway;
    }
}
