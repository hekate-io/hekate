package io.hekate.messaging.operation;

/**
 * Configurer of {@link SendRetryPolicy}.
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
