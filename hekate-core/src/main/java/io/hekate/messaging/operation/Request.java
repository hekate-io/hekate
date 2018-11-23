package io.hekate.messaging.operation;

import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.loadbalance.LoadBalancer;
import java.util.concurrent.TimeUnit;

/**
 * <b>Request operation.</b>
 *
 * <p>
 * This interface represents a bidirectional request operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newRequest(Object)} method call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #submit()} method</li>
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
    Request<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     */
    Request<T> withRetry(RequestRetryConfigurer<T> retry);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    RequestFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void submit(RequestCallback<T> callback) {
        submit().whenComplete((rsp, err) ->
            callback.onComplete(err, rsp)
        );
    }

    /**
     * Synchronously executes this operation and returns the response's payload.
     *
     * @return Response's payload (see {@link Response#payload()}).
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default T response() throws MessagingFutureException, InterruptedException {
        return submit().get().payload();
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
    default <R extends T> R response(Class<R> responseType) throws MessagingFutureException, InterruptedException {
        return submit().get().payload(responseType);
    }

    /**
     * Synchronously executes this operation and returns the response.
     *
     * @return Response.
     *
     * @throws MessagingFutureException if operations fails.
     * @throws InterruptedException if thread got interrupted while awaiting for this operation to complete.
     */
    default Response<T> get() throws MessagingFutureException, InterruptedException {
        return submit().get();
    }
}
