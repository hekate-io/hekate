package io.hekate.messaging.broadcast;

/**
 * Configurer of a retry policy for {@link Broadcast} operations.
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
