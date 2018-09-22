package io.hekate.messaging.intercept;

import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingChannel;
import java.util.Optional;

/**
 * Server's inbound message context.
 *
 * @see MessageInterceptor
 */
public interface ServerReceiveContext {
    /**
     * Type of a send operation.
     *
     * @return Type of a send operation.
     */
    OutboundType type();

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
     * Reads the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> readMetaData();

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
     * Returns the attribute for the specified name.
     *
     * @param name Name.
     *
     * @return Value or {@code null} if there is no such attribute.
     *
     * @see #setAttribute(String, Object)
     */
    Object getAttribute(String name);
}
