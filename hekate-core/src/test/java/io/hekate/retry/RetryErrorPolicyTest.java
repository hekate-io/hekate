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

package io.hekate.retry;

import io.hekate.HekateTestBase;
import io.hekate.messaging.retry.RetryErrorPolicy;
import io.hekate.messaging.retry.RetryFailure;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RetryErrorPolicyTest extends HekateTestBase {
    private final RetryFailure failure = mock(RetryFailure.class);

    @Test
    public void testAlwaysFail() throws Exception {
        assertFalse(RetryErrorPolicy.alwaysFail().shouldRetry(failure));

        verifyNoMoreInteractions(failure);
    }

    @Test
    public void testAlwaysRetry() throws Exception {
        assertTrue(RetryErrorPolicy.alwaysRetry().shouldRetry(failure));

        verifyNoMoreInteractions(failure);
    }
}
