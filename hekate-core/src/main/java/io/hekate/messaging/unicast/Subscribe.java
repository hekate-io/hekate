package io.hekate.messaging.unicast;

import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Subscribe operation.
 *
 * <p>
 * This interface represents a bidirectional subscribe operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newSubscribe(Object)} call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #submit(RequestCallback)}  method</li>
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
     * Response condition.
     *
     * <p>
     * Operation will not be completed unless its results (final response or an error) matches with the specified {@link RequestCondition}.
     * Negative decision will trigger resubmission of a subscription request.
     * </p>
     *
     * @param condition Condition.
     *
     * @return This instance.
     */
    Subscribe<T> until(RequestCondition<T> condition);

    /**
     * Asynchronously executes this operation.
     *
     * @param callback Callback.
     *
     * @return Future result of this operation.
     */
    SubscribeFuture<T> submit(RequestCallback<T> callback);

    /**
     * Synchronously collects all response chunks.
     *
     * <p>
     * This method submits the subscription operation and blocks util all {@link Message#partialReply(Object) responses}) are received.
     * All responses will be collected into an in-memory list, hence this method should be used with caution (mostly for testing purposes).
     * </p>
     *
     * @param timeout Time to wait for operation result.
     * @param unit Time unit of the timeout argument
     *
     * @return List of all {@link Message#partialReply(Object) partial responses} and the {@link Message#reply(Object) final response}.
     *
     * @throws MessagingFutureException if the operation fails.
     * @throws InterruptedException if the current thread is interrupted.
     * @throws TimeoutException if operation times out.
     */
    List<T> collectAll(long timeout, TimeUnit unit) throws InterruptedException, MessagingFutureException, TimeoutException;
}
