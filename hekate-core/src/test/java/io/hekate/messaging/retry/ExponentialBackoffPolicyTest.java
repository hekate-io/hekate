package io.hekate.messaging.retry;

import io.hekate.HekateTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExponentialBackoffPolicyTest extends HekateTestBase {
    @Test
    public void testCustom() throws Exception {
        RetryBackoffPolicy policy = new ExponentialBackoffPolicy(50, 3000);

        assertEquals(0, policy.delayBeforeRetry(0));
        assertEquals(50, policy.delayBeforeRetry(1));
        assertEquals(100, policy.delayBeforeRetry(2));
        assertEquals(200, policy.delayBeforeRetry(3));
        assertEquals(400, policy.delayBeforeRetry(4));
        assertEquals(800, policy.delayBeforeRetry(5));
        assertEquals(1600, policy.delayBeforeRetry(6));
        assertEquals(3000, policy.delayBeforeRetry(7));
        assertEquals(3000, policy.delayBeforeRetry(8));
        assertEquals(3000, policy.delayBeforeRetry(Integer.MAX_VALUE));
    }

    @Test
    public void testDefault() throws Exception {
        RetryBackoffPolicy policy = new ExponentialBackoffPolicy();

        assertEquals(0, policy.delayBeforeRetry(0));
        assertEquals(10, policy.delayBeforeRetry(1));
        assertEquals(20, policy.delayBeforeRetry(2));
        assertEquals(40, policy.delayBeforeRetry(3));
        assertEquals(80, policy.delayBeforeRetry(4));
        assertEquals(160, policy.delayBeforeRetry(5));
        assertEquals(320, policy.delayBeforeRetry(6));
        assertEquals(640, policy.delayBeforeRetry(7));
        assertEquals(1280, policy.delayBeforeRetry(8));
        assertEquals(2560, policy.delayBeforeRetry(9));
        assertEquals(3000, policy.delayBeforeRetry(10));
        assertEquals(3000, policy.delayBeforeRetry(11));
        assertEquals(3000, policy.delayBeforeRetry(Integer.MAX_VALUE));
    }
}
