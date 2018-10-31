package io.hekate.messaging.unicast;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Request operation.
 *
 * <p>
 * This interface represents a bidirectional request operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#request(Object)} method call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #execute()} method</li>
 * <li>Process the response (synchronously or asynchronously)</li>
 * </ol>
 * <h3>Example:</h3>
 * ${source: messaging/MessagingServiceJavadocTest.java#request_operation}
 *
 * @param <T> Message type.
 */
public interface Request<T> {
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
    Request<T> withAffinity(Object affinity);

    /**
     * Response condition.
     *
     * <p>
     * Operation will not be completed unless its results matches with the specified {@link RequestRetryCondition}.
     * </p>
     *
     * @param condition Condition.
     *
     * @return This instance.
     */
    Request<T> until(RequestRetryCondition<T> condition);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    RequestFuture<T> execute();

    /**
     * Synchronously executes this operation and returns the response.
     *
     * @return Response.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default T sync() throws MessagingFutureException, InterruptedException {
        return execute().get().payload();
    }

    /**
     * Synchronously executes this operation and returns the response.
     *
     * @param <R> Response type.
     * @param responseType Response type.
     *
     * @return Response.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default <R extends T> R sync(Class<R> responseType) throws MessagingFutureException, InterruptedException {
        return execute().get().payload(responseType);
    }

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void async(RequestCallback<T> callback) {
        execute().whenComplete((rsp, err) ->
            callback.onComplete(err, rsp)
        );
    }
}
