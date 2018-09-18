package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingEndpoint;
import java.util.Optional;

/**
 * Inbound message context.
 *
 * @param <T> Message type.
 *
 * @see MessageInterceptor#interceptServerReceive(Object, ServerReceiveContext)
 * @see MessageInterceptor#interceptServerSend(Object, ResponseContext, ServerReceiveContext)
 */
public interface ServerReceiveContext<T> {
    /**
     * Returns the original (untransformed) message.
     *
     * @return Message.
     */
    T message();

    /**
     * Type of a send operation.
     *
     * @return Type of a send operation.
     */
    RequestType type();

    /**
     * Messaging endpoint.
     *
     * @return Messaging endpoint.
     */
    MessagingEndpoint<T> endpoint();

    /**
     * Returns the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> metaData();
}
