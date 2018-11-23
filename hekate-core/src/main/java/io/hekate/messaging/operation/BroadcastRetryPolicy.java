package io.hekate.messaging.operation;

import io.hekate.messaging.retry.RetryPolicy;

/**
 * Retry policy for {@link Broadcast} operations.
 *
 * @see Broadcast#withRetry(BroadcastRetryConfigurer)
 */
public interface BroadcastRetryPolicy extends RetryPolicy<BroadcastRetryPolicy> {
    // No-op.
}
