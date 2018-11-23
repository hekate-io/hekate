package io.hekate.messaging.unicast;

/**
 * Configurer of a retry policy for {@link Send} operations.
 *
 * @see Send#withRetry(SendRetryConfigurer)
 */
@FunctionalInterface
public interface SendRetryConfigurer {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(SendRetryPolicy retry);
}
