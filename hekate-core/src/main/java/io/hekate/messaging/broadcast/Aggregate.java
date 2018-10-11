package io.hekate.messaging.broadcast;

import io.hekate.messaging.MessagingChannel;

/**
 * Aggregate operation.
 *
 * <p>
 * This interface represents a bidirectional aggregate operation. Typical use of this interface is:
 * </p>
 * <ol>
 * <li>Obtain an instance of this interface via the {@link MessagingChannel#newAggregate(Object)} method call</li>
 * <li>Set options (f.e. {@link #withAffinity(Object) affinity key})</li>
 * <li>Execute this operation via the {@link #submit()} method</li>
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
    AggregateFuture<T> submit();

    /**
     * Asynchronously executes this operation and notifies the specified callback upon completion.
     *
     * @param callback Callback.
     *
     * @return Future result of this operation.
     */
    default AggregateFuture<T> submit(AggregateCallback<T> callback) {
        AggregateFuture<T> future = submit();

        future.whenComplete((rslt, err) ->
            callback.onComplete(err, rslt)
        );

        return future;
    }
}
