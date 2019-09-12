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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFutureException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RetryGenericPolicyTest extends MessagingServiceTestBase {
    private static final int MAX_ATTEMPTS = 3;

    protected final AtomicInteger failures = new AtomicInteger();

    protected MessagingChannel<String> sender;

    public RetryGenericPolicyTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        List<TestChannel> channels = createAndJoinChannels(2, channel -> channel
            .withRetryPolicy(retry -> retry
                .maxAttempts(MAX_ATTEMPTS)
                .withExponentialDelay(10, 50)
                .onRetry(failure -> {
                    say("Retrying " + failure);
                })
            )
            .withReceiver(msg -> {
                if (failures.getAndDecrement() > 0) {
                    throw TEST_ERROR;
                }

                if (msg.mustReply()) {
                    msg.reply(msg.payload() + "-" + msg.isRetransmit());
                }
            })
        );

        awaitForChannelsTopology(channels);

        sender = channels.get(0).channel().forRemotes();
    }

    @Test
    public void testRequest() throws Exception {
        failures.set(2);

        assertEquals("test-true", sender.request("test"));

        failures.set(4);

        expect(MessagingFutureException.class, () ->
            sender.request("test")
        );
    }

    @Test
    public void testSend() throws Exception {
        failures.set(2);

        sender.send("test");

        failures.set(4);

        expect(MessagingFutureException.class, () ->
            sender.send("test")
        );
    }

    @Test
    public void testSubscribe() throws Exception {
        failures.set(2);

        sender.subscribe("test");

        failures.set(4);

        expect(MessagingFutureException.class, () ->
            sender.subscribe("test")
        );
    }

    @Test
    public void testBroadcast() throws Exception {
        failures.set(2);

        assertTrue(sender.broadcast("test").isSuccess());

        failures.set(4);

        assertFalse(sender.broadcast("test").isSuccess());
    }

    @Test
    public void testAggregate() throws Exception {
        failures.set(2);

        assertTrue(sender.aggregate("test").isSuccess());

        failures.set(4);

        assertFalse(sender.aggregate("test").isSuccess());
    }
}
