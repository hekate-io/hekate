package io.hekate.messaging.intercept;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.retry.RetryFailure;
import java.util.Optional;

/**
 * Client's outbound message context.
 *
 * @param <T> Message type.
 */
public interface ClientOutboundContext<T> {
    /**
     * Returns the type of this message.
     *
     * @return Type of this message.
     */
    OutboundType type();

    /**
     * Returns the outbound message.
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
     * Returns the attribute for the specified name.
     *
     * @param name Name.
     *
     * @return Value or {@code null} if there is no such attribute.
     *
     * @see ClientSendContext#setAttribute(String, Object)
     */
    Object getAttribute(String name);

    /**
     * Target node that was selected by the {@link LoadBalancer}.
     *
     * @return Target node that was selected by the {@link LoadBalancer}.
     */
    ClusterNode receiver();

    /**
     * Cluster topology that was used by the {@link LoadBalancer}.
     *
     * @return Cluster topology that was used by the {@link LoadBalancer}.
     */
    ClusterTopology topology();

    /**
     * Returns <tt>true</tt> if the messaging operation has an affinity key (see {@link #affinityKey()}).
     *
     * @return <tt>true</tt> if the messaging operation has an affinity key.
     */
    boolean hasAffinity();

    /**
     * Returns the hash code of affinity key or a synthetically generated value if affinity key was not specified for the messaging
     * operation.
     *
     * @return Hash code of affinity key.
     */
    int affinity();

    /**
     * Returns the affinity key of the messaging operation or {@code null} if the affinity key wasn't specified.
     *
     * @return Affinity key or {@code null}.
     */
    Object affinityKey();

    /**
     * Returns the previous failure in case if this is a retry attempt.
     *
     * @return Failure of a previous attempt.
     */
    Optional<RetryFailure> prevFailure();
}
