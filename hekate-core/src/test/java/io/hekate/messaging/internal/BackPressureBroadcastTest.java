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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.messaging.MessageQueueOverflowException;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.operation.BroadcastFuture;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Stream;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BackPressureBroadcastTest extends BackPressureTestBase {
    public static final int RECEIVERS = 2;

    public BackPressureBroadcastTest(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<BackPressureTestContext> getBackPressureTestContexts() {
        return getMessagingServiceTestContexts().stream().flatMap(ctx ->
            Stream.of(
                // Multiply lo/hi watermarks by the number of receivers, since each broadcast/aggregate operation takes
                // a number of back pressure slots that is proportional to the number of receiving nodes.
                new BackPressureTestContext(ctx, 0, RECEIVERS),
                new BackPressureTestContext(ctx, 2 * RECEIVERS, 4 * RECEIVERS)
            ))
            .collect(toList());
    }

    @Test
    // TODO: Disabled back-pressure test for broadcast operations.
    @Ignore("Temporary disabled due to the test instability. Need to refactor logic of this test.")
    public void test() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.payload().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.payload().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().channel().forRemotes();

        // Ensure that connection to each node is established.
        sender.newRequest("init").response();

        BroadcastFuture<String> future = null;

        try {
            for (int step = 0; step < 2000000; step++) {
                future = sender.newBroadcast("message-" + step).submit();

                if (future.isDone() && !future.get().isSuccess()) {
                    say("Completed after " + step + " steps.");

                    break;
                }
            }
        } finally {
            resumeReceive.countDown();
        }

        assertNotNull(future);

        Map<ClusterNode, Throwable> errors = future.get().errors();

        assertFalse(errors.isEmpty());
        assertTrue(errors.toString(), errors.values().stream().allMatch(e ->
            ErrorUtils.isCausedBy(MessageQueueOverflowException.class, e))
        );
    }
}
