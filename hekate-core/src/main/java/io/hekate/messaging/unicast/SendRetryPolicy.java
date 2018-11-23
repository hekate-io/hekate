package io.hekate.messaging.unicast;

import io.hekate.messaging.retry.RetryPolicy;
import io.hekate.messaging.retry.RetryRoutingSupport;

/**
 * Retry policy for {@link Send} operations.
 *
 * @see Send#withRetry(SendRetryConfigurer)
 */
public interface SendRetryPolicy extends RetryPolicy<SendRetryPolicy>, RetryRoutingSupport<SendRetryPolicy> {
    // No-op.
}
