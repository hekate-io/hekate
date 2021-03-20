/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.retry;

import io.hekate.HekateTestBase;
import io.hekate.messaging.retry.FixedBackoffPolicy;
import io.hekate.messaging.retry.RetryBackoffPolicy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FixedBackoffPolicyTest extends HekateTestBase {
    @Test
    public void testCustom() {
        RetryBackoffPolicy backoff = new FixedBackoffPolicy(100500);

        for (int i = 0; i < 10; i++) {
            assertEquals(100500, backoff.delayBeforeRetry(i));
        }
    }

    @Test
    public void testDefault() {
        RetryBackoffPolicy backoff = new FixedBackoffPolicy();

        for (int i = 0; i < 10; i++) {
            assertEquals(FixedBackoffPolicy.DEFAULT_DELAY, backoff.delayBeforeRetry(i));
        }
    }
}
