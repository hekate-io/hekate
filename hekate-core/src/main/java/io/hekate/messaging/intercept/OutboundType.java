package io.hekate.messaging.intercept;

import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.Subscribe;

/**
 * Type of an outbound message.
 *
 * @see MessageInterceptor
 */
public enum OutboundType {
    /** Message is submitted as a {@link Request} operation. */
    REQUEST,

    /** Message is submitted as a {@link Subscribe} operation. */
    SUBSCRIBE,

    /**
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(AckMode)} set to {@link AckMode#REQUIRED}.
     */
    SEND_WITH_ACK,

    /**
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(AckMode)} set to {@link AckMode#NOT_NEEDED}.
     */
    SEND_NO_ACK
}
