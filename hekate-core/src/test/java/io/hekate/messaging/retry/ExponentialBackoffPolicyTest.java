/*
 * Copyright 2022 The Hekate Project
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
}
