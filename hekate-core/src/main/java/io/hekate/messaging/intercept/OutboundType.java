package io.hekate.messaging.intercept;

import io.hekate.messaging.unicast.Request;
import io.hekate.messaging.unicast.Send;
import io.hekate.messaging.unicast.SendAckMode;
import io.hekate.messaging.unicast.Subscribe;

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
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(SendAckMode)} set to {@link SendAckMode#REQUIRED}.
     */
    SEND_WITH_ACK,

    /**
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(SendAckMode)} set to {@link SendAckMode#NOT_NEEDED}.
     */
    SEND_NO_ACK
}
