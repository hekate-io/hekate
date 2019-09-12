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
import io.hekate.messaging.operation.RequestFuture;
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
    public void testRequestOrderAfterRetry() throws Exception {
        List<Integer> received = Collections.synchronizedList(new ArrayList<>());

        AtomicInteger processed = new AtomicInteger();

        TestChannel receiver = createChannel(c -> c.withReceiver(msg -> {
            int order = Integer.parseInt(msg.payload());

            int attempt = processed.getAndIncrement();

            if (attempt < 100 && attempt % 10 == 0) {
                throw TEST_ERROR;
            }

            received.add(order);

            msg.reply("ok");
        })).join();

        TestChannel sender = createChannel().join();

        awaitForChannelsTopology(sender, receiver);

        MessagingChannel<String> channel = sender.channel().forRemotes();

        List<RequestFuture<String>> tasks = new ArrayList<>();

        for (int i = 0; i < 100; i++) {
            tasks.add(channel.newRequest(String.valueOf(i))
                .withAffinity(1)
                .withRetry(retry -> retry.maxAttempts(10))
                .submit()
            );
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
