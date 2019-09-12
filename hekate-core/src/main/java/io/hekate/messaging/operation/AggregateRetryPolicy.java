package io.hekate.messaging.operation;

import io.hekate.messaging.retry.RetryPolicy;
import io.hekate.messaging.retry.RetryResponseSupport;

/**
 * Retry policy for an {@link Aggregate} operation.
 *
 * @param <T> Message type.
 *
 * @see Aggregate#withRetry(AggregateRetryConfigurer)
 */
public interface AggregateRetryPolicy<T> extends RetryPolicy<AggregateRetryPolicy<T>>, RetryResponseSupport<T, AggregateRetryPolicy<T>> {
    // No-op.
}
