package io.hekate.messaging.retry;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

/**
 * A backoff policy that waits exponentially longer between each attempt (but keeps a constant delay once a maximum delay is reached).
 *
 * <p>
 * This policy uses the following formula to calculate the backoff delay:
 *
 * <pre>{@code
 *   (2 ^ (attempt -1)) * baseDelay
 * }</pre>
 *
 * <b>Note:</b> for the very first attempt the answer is always {@code 0} (i.e. retry immediately).
 * </p>
 *
 * <p>
 * Parameters of this policy are:
 * </p>
 * <ul>
 * <li>{@code BaseDelay} - multiplier for each attempt</li>
 * <li>{@code MaxDelay} - maximum delay (calculated delay will never exceed this value)</li>
 * </ul>
 *
 * <p>
 * Examples of calculations:
 * </p>
 * <pre>{@code
 * BaseDelay: 10  MaxDelay: 3000
 * -------------------------------
 *  Attempt 0  -> 0
 *  Attempt 1  -> 10
 *  Attempt 2  -> 20
 *  Attempt 3  -> 40
 *  Attempt 4  -> 80
 *  Attempt 5  -> 160
 *  Attempt 6  -> 320
 *  Attempt 7  -> 640
 *  Attempt 8  -> 1280
 *  Attempt 9  -> 2560
 *  Attempt 10 -> 3000
 *  ...        -> 3000
 * }</pre>
 * <pre>{@code
 * BaseDelay: 500  MaxDelay: 30000
 * -------------------------------
 *  Attempt 0 -> 0
 *  Attempt 1 -> 500
 *  Attempt 2 -> 1000
 *  Attempt 3 -> 2000
 *  Attempt 4 -> 4000
 *  Attempt 5 -> 8000
 *  Attempt 6 -> 16000
 *  Attempt 7 -> 30000
 *  ...       -> 30000
 * }</pre>
 */
public class ExponentialBackoffPolicy implements RetryBackoffPolicy {
    /** Default base delay (={@value}) in milliseconds. */
    public static final long DEFAULT_BASE_DELAY = 50;

    /** Default maximum delay (={@value}) in milliseconds. */
    public static final long DEFAULT_MAX_DELAY = 3000;

    /** Base delay. */
    private final long baseDelay;

    /** Maximum delay. */
    private final long maxDelay;

    /** Max attempt limit (max value before calculations overflow). */
    @ToStringIgnore
    private final int attemptOverflow;

    /**
     * Constructs a new instance with default values.
     *
     * @see #DEFAULT_BASE_DELAY
     * @see #DEFAULT_MAX_DELAY
     */
    public ExponentialBackoffPolicy() {
        this(DEFAULT_BASE_DELAY, DEFAULT_MAX_DELAY);
    }

    /**
     * Constructs a new instance.
     *
     * @param baseDelay Multiplier  for each attempt (in milliseconds).
     * @param maxDelay Maximum delay in milliseconds (calculated delay will never exceed this value).
     */
    public ExponentialBackoffPolicy(long baseDelay, long maxDelay) {
        ArgAssert.positive(baseDelay, "Base delay");
        ArgAssert.positive(maxDelay, "Maximum delay");
        ArgAssert.check(baseDelay <= maxDelay, "Base delay can't be less than max delay.");

        this.baseDelay = baseDelay;
        this.maxDelay = maxDelay;

        // Maximum attempts before we start to overflow.
        this.attemptOverflow = Long.SIZE - Long.numberOfLeadingZeros(Long.MAX_VALUE / baseDelay) - 1;
    }

    public static void main(String[] args) {
        long baseDelay = DEFAULT_BASE_DELAY;
        long maxDelay = DEFAULT_MAX_DELAY;

        ExponentialBackoffPolicy p = new ExponentialBackoffPolicy(baseDelay, maxDelay);

        System.out.println("BaseDelay: " + baseDelay + "  MaxDelay: " + maxDelay);
        System.out.println("-------------------------------");

        for (int i = 0; i < 100; i++) {
            long delay = p.delayBeforeRetry(i);

            System.out.println("assertEquals(" + p.delayBeforeRetry(i) + ", policy.delayBeforeRetry(" + i + "));");

            if (delay == maxDelay) {
                break;
            }
        }

        System.out.println("MAX/2 ~ " + p.delayBeforeRetry(Integer.MAX_VALUE / 2));
        System.out.println("MAX ~ " + p.delayBeforeRetry(Integer.MAX_VALUE));
        System.out.println("MIN ~ " + p.delayBeforeRetry(Integer.MIN_VALUE));
    }

    @Override
    public long delayBeforeRetry(int attempt) {
        if (attempt == 0) {
            return 0;
        } else if (attempt >= attemptOverflow) {
            return maxDelay;
        } else {
            int calcAttempt = attempt - 1; // <- First attempt must be always retried immediately.

            return (long)Math.min(Math.pow(2, calcAttempt) * baseDelay, maxDelay);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
