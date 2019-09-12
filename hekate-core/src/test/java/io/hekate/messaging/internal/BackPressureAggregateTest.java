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
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.AggregateResult;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BackPressureAggregateTest extends BackPressureTestBase {
    public static final int RECEIVERS = 2;

    public BackPressureAggregateTest(BackPressureTestContext ctx) {
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
    public void test() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests1::add)
        ).join();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests2::add)
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().channel().forRemotes();

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark / RECEIVERS; i++) {
            futureResponses.add(sender.newAggregate("request-" + i).submit());
        }

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        // Go down to low watermark.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream().filter(r -> r.payload().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests2.stream().filter(r -> r.payload().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        AggregateFuture<String> last = sender.newAggregate("last").submit();

        busyWait("last request received", () -> requests1.size() == futureResponses.size() + 1);
        busyWait("last request received", () -> requests2.size() == futureResponses.size() + 1);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));
        requests2.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        assertTrue(get(last).isSuccess());

        for (Future<?> future : futureResponses) {
            get(future);
        }
    }

    @Test
    public void testFailure() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(requests1::add)
        ).join();

        TestChannel receiver2 = createChannel(c -> useBackPressure(c)
            .withReceiver(requests2::add)
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().channel().forRemotes();

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark / RECEIVERS; i++) {
            futureResponses.add(sender.newAggregate("request-" + i).submit());
        }

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Go down to low watermark on first node.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream()
                .filter(r -> r.payload().equals(request))
                .findFirst()
                .ifPresent(r ->
                    r.reply("ok")
                );
        }

        // Stop second receiver so that all pending requests would partially fail.
        receiver2.leave();

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        AggregateFuture<String> last = sender.newAggregate("last").submit();

        busyWait("last request received", () -> requests1.size() == futureResponses.size() + 1);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        assertTrue(get(last).isSuccess());

        for (Future<?> future : futureResponses) {
            get(future);
        }
    }

    @Test
    public void testContinuousBlocking() throws Exception {
        int remoteNodes = 2;

        for (int i = 0; i < remoteNodes; i++) {
            createChannel(c -> useBackPressure(c)
                .withReceiver(msg -> msg.reply("ok"))
            ).join();
        }

        MessagingChannel<String> sender = createChannel(cfg -> {
            useBackPressure(cfg);
            cfg.getBackPressure().withOutOverflowPolicy(MessagingOverflowPolicy.BLOCK);
        }).join().channel().forRemotes();

        get(sender.cluster().futureOf(topology -> topology.size() == remoteNodes));

        int requests = 1000;

        List<AggregateFuture<String>> asyncResponses = new ArrayList<>(requests);

        for (int i = 0; i < requests; i++) {
            if (i > 0 && i % 100 == 0) {
                say("Submitted requests: %s", i);
            }

            asyncResponses.add(sender.newAggregate("test-" + i).submit());
        }

        for (AggregateFuture<String> future : asyncResponses) {
            AggregateResult<String> result = get(future);

            assertTrue(result.isSuccess());
            assertEquals(2, result.resultsByNode().size());
        }
    }
}
