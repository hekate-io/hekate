package io.hekate.messaging.unicast;

/**
 * Request condition.
 *
 * @param <T> Response type.
 *
 * @see Request#until(RequestCondition)
 */
@FunctionalInterface
public interface RequestCondition<T> {
    /**
     * Called upon a request operation completion (either successfully or with an error) in order decide on whether the operation outcome
     * can be accepted by this callback.
     *
     * <p>
     * For more details about possible decisions please see the documentation of {@link ReplyDecision} enum values.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param reply Response ({@code null} if operation failed).
     *
     * @return Decision.
     */
    ReplyDecision accept(Throwable err, Response<T> reply);
}
