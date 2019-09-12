package io.hekate.messaging.retry;

/**
 * Condition to retry an operation.
 *
 * @see RetryPolicy#whileTrue(RetryCondition)
 */
public interface RetryCondition {
    /**
     * Returns {@code true} if operation should be retried.
     *
     * @return {@code true} if operation should be retried.
     */
    boolean shouldRetry();
}
