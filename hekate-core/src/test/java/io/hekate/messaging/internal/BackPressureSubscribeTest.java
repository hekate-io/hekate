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

import io.hekate.messaging.Message;
import io.hekate.messaging.operation.RequestFuture;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class BackPressureSubscribeTest extends BackPressureParametrizedTestBase {
    public BackPressureSubscribeTest(BackPressureTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testPartialReplyIsGuarded() throws Exception {
        AtomicReference<Message<String>> receivedRef = new AtomicReference<>();

        TestChannel receiver = createChannel(c -> useBackPressure(c)
            .withReceiver(receivedRef::set)
        ).join();

        List<Message<String>> requests = new CopyOnWriteArrayList<>();

        TestChannel sender = createChannel(c -> useBackPressure(c)
            .withReceiver(requests::add)
        ).join();

        awaitForChannelsTopology(sender, receiver);

        // Enforce back pressure on the receiver in order to block sending of partial responses.
        List<RequestFuture<String>> futureResponses = requestUpToHighWatermark(receiver.channel());

        busyWait("requests received", () -> requests.size() == futureResponses.size());

        // Send trigger message.
        sender.channel().forRemotes().newSubscribe("init").submit((err, rsp) -> { /* Ignore. */ });

        // Await for trigger message to be received.
        busyWait("trigger received", () -> receivedRef.get() != null);

        Message<String> received = receivedRef.get();

        // Make sure that back pressure is applied when sending partial reply.
        assertBackPressureOnPartialReply(received);

        // Go down to low watermark.
        requests.stream().limit(getLowWatermarkBounds()).forEach(r -> r.reply("ok"));

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that partial replies can be send.
        CompletableFuture<Throwable> errFuture = new CompletableFuture<>();

        received.partialReply("last", errFuture::complete);

        assertNull(get(errFuture));
    }
}
