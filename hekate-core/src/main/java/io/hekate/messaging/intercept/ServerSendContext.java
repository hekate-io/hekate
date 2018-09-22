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
     * Returns {@code true} if this message has meta-data.
     *
     * @return {@code true} if this message has meta-data.
     *
     * @see #metaData()
     */
    boolean hasMetaData();

    /**
     * Returns the message's meta-data.
     *
     * @return Message meta-data.
     *
     * @see #hasMetaData()
     */
    MessageMetaData metaData();
}
