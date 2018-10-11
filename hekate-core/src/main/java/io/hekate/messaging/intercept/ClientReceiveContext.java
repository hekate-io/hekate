package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;
import java.util.Optional;

/**
 * Client's receive context.
 *
 * @param <T> Message type.
 *
 * @see ClientMessageInterceptor#interceptClientReceiveResponse(ClientReceiveContext)
 */
public interface ClientReceiveContext<T> {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();

    /**
     * Returns the inbound message.
     *
     * @return Message.
     */
    T get();

    /**
     * Returns the outbound context.
     *
     * @return Outbound context.
     */
    ClientOutboundContext<T> outboundContext();

    /**
     * Overrides the received message with the specified one.
     *
     * @param msg New message that should replace the received one.
     */
    void overrideMessage(T msg);

    /**
     * Reads the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> readMetaData();
}
