package io.hekate.messaging.intercept;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.ResponseCallback;
import io.hekate.messaging.unicast.SendCallback;

/**
 * Type of an outbound message.
 *
 * @see MessageInterceptor
 */
public enum OutboundType {
    /** Message is submitted via {@link MessagingChannel#request(Object, ResponseCallback)}. */
    REQUEST,

    /** Message is submitted via  {@link MessagingChannel#subscribe(Object, ResponseCallback)}. */
    SUBSCRIBE,

    /**
     * Message is submitted via {@link MessagingChannel#send(Object, SendCallback)} and {@link MessagingChannel#withConfirmReceive(boolean)}
     * set to {@code true}.
     */
    SEND_WITH_ACK,

    /**
     * Message is submitted via {@link MessagingChannel#send(Object, SendCallback)} and {@link MessagingChannel#withConfirmReceive(boolean)}
     * set to {@code false}.
     */
    SEND_NO_ACK
}
