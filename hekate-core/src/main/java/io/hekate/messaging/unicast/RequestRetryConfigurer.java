package io.hekate.messaging.unicast;

/**
 * Configurer of a retry policy for {@link Request} and {@link Subscribe} operations.
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
