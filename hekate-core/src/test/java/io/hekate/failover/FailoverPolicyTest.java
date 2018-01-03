/*
 * Copyright 2018 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.failover;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FailoverPolicyTest extends FailoverPolicyTestBase {
    @Test
    public void testAlwaysFail() throws Exception {
        FailoverPolicy policy = FailoverPolicy.alwaysFail();

        FailureResolution resolution = policy.apply(newContext(0));

        assertFalse(resolution.isRetry());
    }

    @Test
    public void testMaxFailoverAttempts() throws Exception {
        MaxFailoverAttempts attempts = new MaxFailoverAttempts(2);

        assertTrue(attempts.test(newContext(0)));
        assertTrue(attempts.test(newContext(1)));

        assertFalse(attempts.test(newContext(2)));
        assertFalse(attempts.test(newContext(3)));

        assertTrue(attempts.toString(), attempts.toString().startsWith(MaxFailoverAttempts.class.getSimpleName()));
    }

    @Test
    public void testConstantFailoverDelay() throws Exception {
        ConstantFailoverDelay delay = new ConstantFailoverDelay(99);

        assertEquals(99, delay.delayOf(newContext(0)));
        assertEquals(99, delay.delayOf(newContext(1)));
        assertEquals(99, delay.delayOf(newContext(2)));

        assertTrue(delay.toString(), delay.toString().startsWith(ConstantFailoverDelay.class.getSimpleName()));
    }
}
