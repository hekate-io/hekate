package io.hekate.messaging.retry;

import io.hekate.messaging.operation.Response;

/**
 * Policy to retry upon an unexpected response.
 *
 * @param <T> Response type.
 *
 * @see RetryResponseSupport
 */
@FunctionalInterface
public interface RetryResponsePolicy<T> {
    /**
     * Called upon a request operation completion in order decide on whether the operation outcome can be accepted or should be retried.
     *
     * <ul>
     * <li>{@code true} - Signals that operation should be retried</li>
     * <li>{@code false} - signals that response is accepted and operation should be completed</li>
     * </ul>
     *
     * @param rsp Response.
     *
     * @return Decision.
     */
    boolean shouldRetry(Response<T> rsp);
}
