package io.hekate.messaging.operation;

/**
 * Configurer of {@link BroadcastRetryPolicy}.
 *
 * @see Broadcast#withRetry(BroadcastRetryConfigurer)
 */
@FunctionalInterface
public interface BroadcastRetryConfigurer {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(BroadcastRetryPolicy retry);
}
