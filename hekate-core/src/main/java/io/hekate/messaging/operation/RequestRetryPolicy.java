package io.hekate.messaging.operation;

import io.hekate.messaging.retry.RetryPolicy;
import io.hekate.messaging.retry.RetryResponseSupport;
import io.hekate.messaging.retry.RetryRoutingSupport;

/**
 * Retry policy for {@link Request} and {@link Subscribe} operations.
 *
 * @param <T> Message type.
 *
 * @see Request#withRetry(RequestRetryConfigurer)
 * @see Subscribe#withRetry(RequestRetryConfigurer)
 */
public interface RequestRetryPolicy<T> extends RetryPolicy<RequestRetryPolicy<T>>, RetryRoutingSupport<RequestRetryPolicy<T>>,
    RetryResponseSupport<T, RequestRetryPolicy<T>> {
    // No-op.
}
