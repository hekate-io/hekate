package io.hekate.retry;

import io.hekate.HekateTestBase;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RetryBackoffPolicyTest extends HekateTestBase {
    @Test
    public void testFixedDelay() {
        RetryBackoffPolicy backoff = RetryBackoffPolicy.fixedDelay(100500);

        for (int i = 0; i < 10; i++) {
            assertEquals(100500, backoff.delayBeforeRetry(i));
        }
    }

    @Test
    public void testDefaultFixedDelay() {
        RetryBackoffPolicy backoff = RetryBackoffPolicy.defaultFixedDelay();

        for (int i = 0; i < 10; i++) {
            assertEquals(RetryBackoffPolicy.DEFAULT_FIXED_DELAY, backoff.delayBeforeRetry(i));
        }
    }
}
