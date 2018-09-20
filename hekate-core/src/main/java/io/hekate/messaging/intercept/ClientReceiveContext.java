package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;
import java.util.Optional;

/**
 * Client's inbound message context.
 *
 * @see MessageInterceptor
 */
public interface ClientReceiveContext {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();

    /**
     * Reads the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> readMetaData();
}
