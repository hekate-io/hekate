package io.hekate.messaging.retry;

/**
 * Backoff policy.
 */
public interface RetryBackoffPolicy {
    /**
     * Returns a delay in milliseconds before the next retry attempt.
     *
     * @param attempt Attempt (starting with zero for the very first attempt).
     *
     * @return Delay in milliseconds.
     */
    long delayBeforeRetry(int attempt);
}
