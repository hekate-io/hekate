package io.hekate.failover;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MaxFailoverAttemptsTest extends FailoverPolicyTestBase {
    @Test
    public void testMaxFailoverAttempts() throws Exception {
        MaxFailoverAttempts attempts = new MaxFailoverAttempts(2);

        assertTrue(attempts.test(newContext(0)));
        assertTrue(attempts.test(newContext(1)));

        assertFalse(attempts.test(newContext(2)));
        assertFalse(attempts.test(newContext(3)));

        assertTrue(attempts.toString(), attempts.toString().startsWith(MaxFailoverAttempts.class.getSimpleName()));
    }
}
