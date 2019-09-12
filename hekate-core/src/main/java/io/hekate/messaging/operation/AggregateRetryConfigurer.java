package io.hekate.messaging.operation;

/**
 * Configurer of {@link AggregateRetryPolicy}.
 *
 * @param <T> Message type.
 *
 * @see Aggregate#withRetry(AggregateRetryConfigurer)
 */
@FunctionalInterface
public interface AggregateRetryConfigurer<T> {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(AggregateRetryPolicy<T> retry);
}
