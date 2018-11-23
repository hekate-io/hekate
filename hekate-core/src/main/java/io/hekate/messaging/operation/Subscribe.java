package io.hekate.messaging.operation;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <b>Subscribe operation.</b>
 *
 * <p>
 * This interface represents a bidirectional subscribe operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newSubscribe(Object)} call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #submit(SubscribeCallback)}  method</li>
 * <li>Await for the execution result, if needed</li>
 * </ol>
 * <h3>Example:</h3>
 * ${source: messaging/MessagingServiceJavadocTest.java#subscribe_operation}
 *
 * @param <T> Message type.
 */
public interface Subscribe<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * <p>
     * {@link LoadBalancer} can also make use of the affinity key in order to perform consistent routing of messages among the cluster
     * node. For example, the default load balancer makes sure that all messages, having the same key, are always routed to the same node
     * (unless the cluster topology doesn't change).
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */
    Subscribe<T> withAffinity(Object affinity);

    /**
     * Overrides the channel's default timeout value for this operation.
     *
     * <p>
     * If this operation can not complete at the specified timeout then this operation will end up the the {@link MessageTimeoutException}.
     * </p>
     *
     * <p>
     * Specifying a negative or zero value disables the timeout check.
     * </p>
     *
     * @param timeout Timeout.
     * @param unit Unit.
     *
     * @return This instance.
     *
     * @see MessagingChannelConfig#setMessagingTimeout(long)
     */
    Subscribe<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     */
    Subscribe<T> withRetry(RequestRetryConfigurer<T> retry);

    /**
     * Asynchronously executes this operation.
     *
     * @param callback Callback.
     *
     * @return Future result of this operation.
     */
    SubscribeFuture<T> submit(SubscribeCallback<T> callback);

    /**
     * Synchronously collects all response chunks.
     *
     * <p>
     * This method submits the subscription operation and blocks util all {@link Message#partialReply(Object) responses}) are received.
     * All responses will be collected into an in-memory list, hence this method should be used with caution (mostly for testing purposes).
     * </p>
     *
     * @return List of all {@link Message#partialReply(Object) partial responses} and the {@link Message#reply(Object) final response}.
     *
     * @throws MessagingFutureException if the operation fails.
     * @throws InterruptedException if the current thread is interrupted.
     */
    List<T> responses() throws InterruptedException, MessagingFutureException;
}
