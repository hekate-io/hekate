package io.hekate.messaging.retry;

import io.hekate.messaging.MessagingChannelConfig;

/**
 * Base interface for retry policies.
 *
 * @param <P> Policy type.
 */
public interface RetryPolicy<P extends RetryPolicy<P>> {
    /**
     * Backoff policy.
     *
     * <p>
     * This method overrides the channel's default backoff policy (see {@link MessagingChannelConfig#setBackoffPolicy(RetryBackoffPolicy)}).
     * </p>
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
