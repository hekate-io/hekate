package io.hekate.messaging.intercept;

import io.hekate.messaging.MessageMetaData;

/**
 * Client's send context.
 *
 * @param <T> Message type.
 *
 * @see MessageInterceptor
 */
public interface ClientSendContext<T> extends ClientOutboundContext<T> {
    /**
     * Returns the message's meta-data.
     *
     * @return Message meta-data.
     */
    MessageMetaData metaData();

    /**
     * Overrides the message to be sent with the specified one.
     *
     * @param msg New message that should be sent instead of the original one.
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

    /**
     * Returns {@code true} if message has a non-empty {@link #metaData()}.
     *
     * @return {@code true} if message has a non-empty {@link #metaData()}.
     */
    boolean hasMetaData();

}
