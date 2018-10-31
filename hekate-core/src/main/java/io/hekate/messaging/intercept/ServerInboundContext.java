package io.hekate.messaging.intercept;

import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessagingChannel;

/**
 * Server's inbound message context.
 *
 * @param <T> Message type.
 */
public interface ServerInboundContext<T> {
    /**
     * Type of a send operation.
     *
     * @return Type of a send operation.
     */
    OutboundType type();

    /**
     * Returns the inbound message.
     *
     * @return Message.
     */
    T payload();

    /**
     * Returns the channel name (see {@link MessagingChannel#name()}).
     *
     * @return Channel name.
     */
    String channelName();

    /**
     * Address of a remote node.
     *
     * @return Address of a remote node.
     */
    ClusterAddress from();

    /**
     * Returns the attribute for the specified name.
     *
     * @param name Name.
     *
     * @return Value or {@code null} if there is no such attribute.
     *
     * @see ServerReceiveContext#setAttribute(String, Object)
     */
    Object getAttribute(String name);
}
