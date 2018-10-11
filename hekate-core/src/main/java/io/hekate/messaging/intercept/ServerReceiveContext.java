package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;
import java.util.Optional;

/**
 * Server's receive context.
 *
 * @param <T> Message type.
 *
 * @see ServerMessageInterceptor#interceptServerReceive(ServerReceiveContext)
 */
public interface ServerReceiveContext<T> extends ServerInboundContext<T> {
    /**
     * Reads the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> readMetaData();

    /**
     * Overrides the received message with the specified one.
     *
     * @param msg New message that should replace the received one.
     */
    void overrideMessage(T msg);

    /**
     * Sets an attribute of this context.
     *
     * <p>
     * Attributes are local to this context object and do not get transferred to a remote peer.
     * </p>
     *
     * @param name Name.
     * @param value Value.
     *
     * @return Previous value or {@code null} if attribute didn't have any value.
     *
     * @see #getAttribute(String)
     */
    Object setAttribute(String name, Object value);

}
