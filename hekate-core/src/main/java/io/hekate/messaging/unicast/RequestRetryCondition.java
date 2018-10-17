package io.hekate.messaging.unicast;

/**
 * Request retry condition.
 *
 * @param <T> Response type.
 *
 * @see Request#until(RequestRetryCondition)
 */
@FunctionalInterface
public interface RequestRetryCondition<T> {
    /**
     * Called upon a request operation completion (either successfully or with an error) in order decide on whether the operation outcome
     * can be accepted.
     *
     * <p>                        
     * For more details about possible decisions please see the documentation of {@link RetryDecision} enum values.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param reply Response ({@code null} if operation failed).
     *
     * @return Decision.
     */
    RetryDecision accept(Throwable err, Response<T> reply);
}
