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
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RetryPolicyTest extends HekateTestBase {
    private RetryPolicy<?> policy;

    @Before
    public void setUp() throws Exception {
        policy = mock(RetryPolicy.class);
    }

    @Test
    public void testFixedDelay() {
        when(policy.withFixedDelay(anyLong())).thenCallRealMethod();

        policy.withFixedDelay(100500);

        verify(policy).withBackoff(argThat(arg -> arg.delayBeforeRetry(1) == 100500));
    }

    @Test
    public void testExponentialDelay() {
        when(policy.withExponentialDelay(anyLong(), anyLong())).thenCallRealMethod();

        policy.withExponentialDelay(100500, 500100);

        verify(policy).withBackoff(argThat(arg -> {
            ExponentialBackoffPolicy exp = (ExponentialBackoffPolicy)arg;

            return exp.baseDelay() == 100500 && exp.maxDelay() == 500100;
        }));
    }

    @Test
    public void testUnlimitedAttempts() {
        when(policy.unlimitedAttempts()).thenCallRealMethod();

        policy.unlimitedAttempts();

        verify(policy).maxAttempts(eq(-1));
    }
}
