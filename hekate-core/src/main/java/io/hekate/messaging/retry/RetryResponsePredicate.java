package io.hekate.messaging.retry;

import io.hekate.messaging.operation.Response;

/**
 * Predicate to test if particular response should be retried.
 *
 * @param <T> Response type.
 *
 * @see RetryResponseSupport
 */
@FunctionalInterface
public interface RetryResponsePredicate<T> {
    /**
     * Called upon a request operation completion in order decide on whether the result is acceptable or should the operation be retried.
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
