package io.hekate.messaging.retry;

/**
 * Backoff policy.
 */
public interface RetryBackoffPolicy {
    /** Delay value (={@value}) in milliseconds for {@link #defaultFixedDelay()}. */
    long DEFAULT_FIXED_DELAY = 100;

    /**
     * Returns a delay in milliseconds before the next retry attempt.
     *
     * @param attempt Attempt (starting with zero for the very first attempt).
     *
     * @return Delay in milliseconds.
     */
    long delayBeforeRetry(int attempt);

    /**
     * Default policy with a fixed delay of {@value #DEFAULT_FIXED_DELAY} milliseconds.
     *
     * @return Fixed delay ({@value #DEFAULT_FIXED_DELAY} milliseconds) policy.
     */
    static RetryBackoffPolicy defaultFixedDelay() {
        return attempt -> DEFAULT_FIXED_DELAY;
    }

    /**
     * Policy that will always use the fixed delay.
     *
     * @param delay Fixed delay.
     *
     * @return Policy with the fixed delay.
     */
    static RetryBackoffPolicy fixedDelay(long delay) {
        return attempt -> delay;
    }
}
