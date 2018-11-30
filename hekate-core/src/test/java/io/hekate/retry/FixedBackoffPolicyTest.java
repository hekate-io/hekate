package io.hekate.retry;

import io.hekate.HekateTestBase;
import io.hekate.messaging.retry.FixedBackoffPolicy;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FixedBackoffPolicyTest extends HekateTestBase {
    @Test
    public void testFixedDelay() {
        RetryBackoffPolicy backoff = new FixedBackoffPolicy(100500);

        for (int i = 0; i < 10; i++) {
            assertEquals(100500, backoff.delayBeforeRetry(i));
        }
    }

    @Test
    public void testDefaultFixedDelay() {
        RetryBackoffPolicy backoff = new FixedBackoffPolicy();

        for (int i = 0; i < 10; i++) {
            assertEquals(FixedBackoffPolicy.DEFAULT_DELAY, backoff.delayBeforeRetry(i));
        }
    }
}
