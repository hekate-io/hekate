package io.hekate.messaging.retry;

/**
 * Base interface for retry policies.
 *
 * @param <P> Policy type.
 */
public interface RetryPolicy<P extends RetryPolicy<P>> {
    /**
     * Retry while this condition is {@code true}.
     *
     * @param condition Condition.
     *
     * @return This instance.
     */
    P whileTrue(RetryCondition condition);

    /**
     * Registers a predicate to control if the operation should to be repeated upon the error.
     *
     * <p>
     * If such a predicate is not registered, the operation will be repeated for any error.
     * </p>
     *
     * @param predicate Predicate.
     *
     * @return This instance.
     */
    P whileError(RetryErrorPolicy predicate);

    /**
     * Registers a callback to be notified when this policy decides to retry a failed operation.
     *
     * @param callback Callback.
     *
     * @return This instance.
     */
    P onRetry(RetryCallback callback);

    /**
     * Sets the maximum number of retry attempts.
     *
     * @param maxAttempts Maximum number of attempts (zero to disable retries, negative value for unlimited attempts).
     *
     * @return This instance.
     */
    P maxAttempts(int maxAttempts);

    /**
     * Retry with unlimited number of attempts.
     *
     * @return Policy to retry with unlimited number of attempts.
     */
    default P unlimitedAttempts() {
        return maxAttempts(-1);
    }
}
