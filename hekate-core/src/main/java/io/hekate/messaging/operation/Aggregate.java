package io.hekate.messaging.operation;

import io.hekate.messaging.MessageTimeoutException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingFutureException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * <b>Aggregate operation.</b>
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
    Aggregate<T> withTimeout(long timeout, TimeUnit unit);

    /**
     * Retry policy.
     *
     * @param retry Retry policy.
     *
     * @return This instance.
     */
    Aggregate<T> withRetry(AggregateRetryConfigurer<T> retry);

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
     */
    default void submit(AggregateCallback<T> callback) {
        submit().whenComplete((rslt, err) ->
            callback.onComplete(err, rslt)
        );
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result (see {@link AggregateResult#results()}).
     *
     * @throws MessagingFutureException If operations fails.
     * @throws InterruptedException If thread got interrupted while awaiting for this operation to complete.
     */
    default Collection<T> results() throws MessagingFutureException, InterruptedException {
        return submit().get().results();
    }

    /**
     * Synchronously executes this operation and returns the result.
     *
     * @return Result.
     *
     * @throws MessagingFutureException If operations fails.
     * @throws InterruptedException If thread got interrupted while awaiting for this operation to complete.
     */
    default AggregateResult<T> get() throws MessagingFutureException, InterruptedException {
        return submit().get();
    }
}
