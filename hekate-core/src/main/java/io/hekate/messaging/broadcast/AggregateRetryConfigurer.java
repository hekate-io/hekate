package io.hekate.messaging.broadcast;

/**
 * Configurer of a retry policy for {@link Aggregate} operations.
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
