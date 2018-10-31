package io.hekate.messaging.broadcast;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;

/**
 * Aggregate operation.
 *
 * <p>
 * This interface represents a bidirectional aggregate operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#aggregate(Object)} method call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #execute()} method</li>
 * <li>Process results (synchronously or asynchronously)</li>
 * </ol>
 * <h3>Example:</h3>
 * ${source: messaging/MessagingServiceJavadocTest.java#aggregate_operation}
 *
 * @param <T> Message type.
 */
public interface Aggregate<T> {
    /**
     * Affinity key.
     *
     * <p>
     * Specifying an affinity key ensures that all operation with the same key will always be transmitted over the same network
     * connection and will always be processed by the same thread.
     * </p>
     *
     * @param affinity Affinity key.
     *
     * @return This instance.
     */

    Aggregate<T> withAffinity(Object affinity);

    /**
     * Asynchronously executes this operation.
     *
     * @return Future result of this operation.
     */
    AggregateFuture<T> execute();

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws MessagingFutureException If operations fails.
     * @throws InterruptedException If thread got interrupted while awaiting for this operation to complete.
     */
    default AggregateResult<T> sync() throws MessagingFutureException, InterruptedException {
        return execute().get();
    }

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     */
    default void async(AggregateCallback<T> callback) {
        execute().whenComplete((rslt, err) ->
            callback.onComplete(err, rslt)
        );
    }
}
