package io.hekate.messaging.intercept;

import io.hekate.messaging.Message;
import io.hekate.messaging.unicast.SendCallback;

/**
 * Type of an inbound message.
 *
 * @see MessageInterceptor
 */
public enum InboundType {
    /** Chunk of a bigger response (see {@link Message#partialReply(Object, SendCallback)}). */
    RESPONSE_CHUNK,

    /** Final response (See {@link Message#reply(Object, SendCallback)}). */
    FINAL_RESPONSE
}
