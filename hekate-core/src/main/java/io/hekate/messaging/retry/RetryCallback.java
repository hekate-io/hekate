package io.hekate.messaging.retry;

/**
 * Callback for receiving retry notifications.
 *
 * @see RetryPolicy#onRetry(RetryCallback)
 */
@FunctionalInterface
public interface RetryCallback {
    /**
     * Gets called when {@link RetryPolicy} decides to retry a failed operation.
     *
     * @param failure Failure.
     */
    void onRetry(FailedAttempt failure);
}
