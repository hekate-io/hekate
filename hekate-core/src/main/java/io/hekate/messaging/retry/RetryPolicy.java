package io.hekate.messaging.retry;

/**
 * Base interface for retry policies.
 *
 * @param <P> Policy type.
 */
public interface RetryPolicy<P extends RetryPolicy<P>> {
    /**
     * Backoff policy.
     *
     * @param backoff Backoff policy.
     *
     * @return This instance.
     */
    P withBackoff(RetryBackoffPolicy backoff);

    /**
     * Retry while this condition is {@code true}.
     *
     * @param condition Condition.
     *
     * @return This instance.
     */
    P whileTrue(RetryCondition condition);

    /**
     * Registers a predicate to control if the operation should be retried upon an error.
     *
     * <p>
     * If such a predicate is not registered, the operation will be retried for any error.
     * </p>
     *
     * @param predicate Predicate.
     *
     * @return This instance.
     */
    P whileError(RetryErrorPredicate predicate);

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
     * Use {@link FixedBackoffPolicy} with the specified delay (in milliseconds).
     *
     * @param delay Delay in milliseconds.
     *
     * @return This instance.
     */
    default P withFixedDelay(long delay) {
        return withBackoff(new FixedBackoffPolicy(delay));
    }

    /**
     * Use {@link ExponentialBackoffPolicy} with the specified base/max delays (in milliseconds).
     *
     * @param baseDelay Multiplier  for each attempt (in milliseconds).
     * @param maxDelay Maximum delay in milliseconds (calculated delay will never exceed this value).
     *
     * @return This instance.
     */
    default P withExponentialDelay(long baseDelay, long maxDelay) {
        return withBackoff(new ExponentialBackoffPolicy(baseDelay, maxDelay));
    }

    /**
     * Retry with unlimited number of attempts.
     *
     * @return Policy to retry with unlimited number of attempts.
     */
    default P unlimitedAttempts() {
        return maxAttempts(-1);
    }
}
