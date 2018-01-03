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

package io.hekate.messaging.internal;

import io.hekate.failover.FailoverPolicyBuilder;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.ResponseFuture;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Ignore;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

// TODO: Implement strict ordering of messages.
@Ignore("Disabled unless strict ordering of messaging is supported.")
public class RequestOrderTest extends MessagingServiceTestBase {
    public RequestOrderTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testRequestOrderAfterFailover() throws Exception {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        AtomicInteger processed = new AtomicInteger();

        TestChannel receiver = createChannel(c -> c.withReceiver(msg -> {
            int order = Integer.parseInt(msg.get());

            int attempt = processed.getAndIncrement();

            if (attempt < 100 && attempt % 10 == 0) {
                throw TEST_ERROR;
            }

            received.add(order);

            msg.reply("ok");
        })).join();

        TestChannel sender = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        MessagingChannel<String> out = sender.get()
            .forRemotes()
            .withFailover(new FailoverPolicyBuilder()
                .withRetryUntil(failover -> true)
                .withErrorTypes(Throwable.class)
                .withConstantRetryDelay(100)
                .withMaxAttempts(10)
            );

        List<ResponseFuture<String>> tasks = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            tasks.add(out.withAffinity(1).request(String.valueOf(i)));
        }

        tasks.forEach(CompletableFuture::join);

        say("Received order: " + received);

        List<Integer> distinct = received.stream().distinct().collect(toList());

        say("      Distinct: " + distinct);

        for (int i = 0; i < distinct.size(); i++) {
            assertEquals(i, distinct.get(i).intValue());
        }
    }
}
