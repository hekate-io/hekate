package io.hekate.messaging.operation;

/**
 * Configurer of {@link RequestRetryPolicy}.
 *
 * @param <T> Message type.
 *
 * @see Request#withRetry(RequestRetryConfigurer)
 * @see Subscribe#withRetry(RequestRetryConfigurer)
 */
@FunctionalInterface
public interface RequestRetryConfigurer<T> {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(RequestRetryPolicy<T> retry);
}
