package io.hekate.messaging.retry;

import io.hekate.util.format.ToString;

/**
 * Fixed delay policy.
 */
public class FixedBackoffPolicy implements RetryBackoffPolicy {
    /** Default delay (={@value}) in milliseconds. */
    public static final long DEFAULT_DELAY = 100;

    /** Delay. */
    private final long delay;

    /**
     * Constructs a new instance with the default delay (={@value #DEFAULT_DELAY}).
     */
    public FixedBackoffPolicy() {
        this(DEFAULT_DELAY);
    }

    /**
     * Constructs a new instance.
     *
     * @param delay Delay.
     */
    public FixedBackoffPolicy(long delay) {
        this.delay = delay;
    }

    @Override
    public long delayBeforeRetry(int attempt) {
        return delay;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
