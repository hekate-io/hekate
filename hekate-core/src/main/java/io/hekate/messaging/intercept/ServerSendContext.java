package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;

/**
 * Server's outbound message context.
 *
 * @see MessageInterceptor
 */
public interface ServerSendContext {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();

    /**
     * Returns the message's meta-data.
     *
     * @return Message meta-data.
     */
    MessageMetaData metaData();
}
