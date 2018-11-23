package io.hekate.messaging.retry;

/**
 * Template interface for policies that can retry upon an unexpected response.
 *
 * @param <P> Policy type.
 * @param <T> Response type.
 */
public interface RetryResponseSupport<T, P extends RetryResponseSupport<T, P>> {
    /**
     * Registers a predicate to control if the operation should to be repeated upon the response.
     *
     * @param predicate Predicate.
     *
     * @return This instance.
     */
    P whileResponse(RetryResponsePolicy<T> predicate);
}
