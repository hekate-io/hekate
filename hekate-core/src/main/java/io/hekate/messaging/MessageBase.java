package io.hekate.messaging;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;

/**
 * Base interface for messages.
 *
 * @param <T> Payload type.
 */
public interface MessageBase<T> {
    /**
     * Returns the payload of this message.
     *
     * @return Payload.
     */
    T get();

    /**
     * Casts the payload of this message ot the specified type.
     *
     * @param type Payload type.
     * @param <P> Payload type.
     *
     * @return Payload.
     *
     * @throws ClassCastException If payload can't be cast to the specified type.
     */
    <P extends T> P get(Class<P> type);

    /**
     * Returns {@code true} if this message has a {@link #get() payload} of the specified type.
     *
     * @param type Payload type.
     *
     * @return {@code true} if this message has a {@link #get() payload} of the specified type.
     */
    boolean is(Class<? extends T> type);

    /**
     * Returns the messaging endpoint of this message..
     *
     * @return Messaging endpoint.
     */
    MessagingEndpoint<T> endpoint();

    /**
     * Returns the messaging channel of this message.
     *
     * @return Messaging channel.
     */
    default MessagingChannel<T> channel() {
        return endpoint().channel();
    }

    /**
     * Returns the universally unique identifier of the remote cluster node.
     *
     * @return Universally unique identifier of the remote cluster node.
     *
     * @see ClusterNode#id()
     */
    default ClusterNodeId from() {
        return endpoint().remoteNodeId();
    }
}
