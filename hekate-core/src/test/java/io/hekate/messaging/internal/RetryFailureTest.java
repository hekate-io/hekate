/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingRemoteException;
import io.hekate.messaging.retry.FailedAttempt;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class RetryFailureTest extends RetryTestBase {
    public RetryFailureTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<FailedAttempt> failures = Collections.synchronizedList(new ArrayList<>());

        this.failures.set(3);

        toRemote.newRequest("test--1")
            .withRetry(retry -> retry
                .unlimitedAttempts()
                .onRetry(failures::add)
            )
            .response();

        assertEquals(3, failures.size());

        for (int i = 0; i < failures.size(); i++) {
            FailedAttempt failure = failures.get(i);

            assertEquals(i, failure.attempt());
            assertSame(MessagingRemoteException.class, failure.error().getClass());
            assertEquals(receiver.node().localNode(), failure.lastTriedNode());
        }
    }
}
