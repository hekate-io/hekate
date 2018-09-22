package io.hekate.messaging.intercept;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.failover.FailoverPolicy;
import io.hekate.failover.FailureInfo;
import io.hekate.messaging.MessageMetaData;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;
import java.util.Optional;

/**
 * Client's outbound message context.
 *
 * @see MessageInterceptor
 */
public interface ClientSendContext {
    /**
     * Returns the type of this message.
     *
     * @return Type of this message.
     */
    OutboundType type();

    /**
     * Returns the channel name (see {@link MessagingChannel#name()}).
     *
     * @return Channel name.
     */
    String channelName();

    /**
     * Returns the message's meta-data.
     *
     * @return Message meta-data.
     */
    MessageMetaData metaData();

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

    /**
     * Returns {@code true} if message has a non-empty {@link #metaData()}.
     *
     * @return {@code true} if message has a non-empty {@link #metaData()}.
     */
    boolean hasMetaData();

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
     *
     * @see MessagingChannel#withAffinity(Object)
     */
    Object affinityKey();

    /**
     * Returns the previous failure in case if this is a failover attempt.
     *
     * @return Failure of a previous attempt.
     *
     * @see MessagingChannel#withFailover(FailoverPolicy)
     */
    Optional<FailureInfo> failure();
}
