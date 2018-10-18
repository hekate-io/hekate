package io.hekate.failover;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BackoffPolicyTest extends HekateTestBase {
    @Test
    public void testFixedDelay() {
        BackoffPolicy backoff = BackoffPolicy.fixedDelay(100500);

        for (int i = 0; i < 10; i++) {
            assertEquals(100500, backoff.delayBeforeRetry(i));
        }
    }

    @Test
    public void testDefaultFixedDelay() {
        BackoffPolicy backoff = BackoffPolicy.defaultFixedDelay();

        for (int i = 0; i < 10; i++) {
            assertEquals(BackoffPolicy.DEFAULT_FIXED_DELAY, backoff.delayBeforeRetry(i));
        }
    }
}
