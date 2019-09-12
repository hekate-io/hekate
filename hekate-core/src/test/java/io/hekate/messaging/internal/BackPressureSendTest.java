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

import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import io.hekate.messaging.operation.SendFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BackPressureSendTest extends BackPressureParametrizedTestBase {
    public BackPressureSendTest(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg ->
            await(resumeReceive, 10)
        )).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().channel().forRemotes();

        // Ensure that sender -> receiver connection is established.
        get(sender.newSend("init").submit());

        SendFuture future = null;

        try {
            for (int step = 0; step < 200000; step++) {
                future = sender.newSend("message-" + step).submit();

                if (future.isCompletedExceptionally()) {
                    say("Completed after " + step + " steps.");

                    break;
                }
            }
        } finally {
            resumeReceive.countDown();
        }

        assertNotNull(future);
        assertTrue(future.isCompletedExceptionally());

        try {
            future.get();

            fail("Error was expected");
        } catch (MessagingFutureException e) {
            assertTrue(ErrorUtils.stackTrace(e), e.isCausedBy(MessageQueueOverflowException.class));
        }
    }
}
