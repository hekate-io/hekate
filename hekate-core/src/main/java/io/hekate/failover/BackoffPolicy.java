package io.hekate.failover;

/**
 * Backoff policy.
 */
public interface BackoffPolicy {
    /** Delay value (={@value}) in milliseconds for {@link #defaultFixedDelay()}. */
    long DEFAULT_FIXED_DELAY = 100;

    /**
     * Returns a delay in milliseconds before the next retry/failover attempt.
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
    static BackoffPolicy defaultFixedDelay() {
        return attempt -> DEFAULT_FIXED_DELAY;
    }

    /**
     * Policy that will always use the fixed delay.
     *
     * @param delay Fixed delay.
     *
     * @return Policy with the fixed delay.
     */
    static BackoffPolicy fixedDelay(long delay) {
        return attempt -> delay;
    }
}
