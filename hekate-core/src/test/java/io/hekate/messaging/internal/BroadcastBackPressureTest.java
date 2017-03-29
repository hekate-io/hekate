/*
 * Copyright 2017 The Hekate Project
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
import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingOverflowException;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.BroadcastFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class BroadcastBackPressureTest extends BackPressureTestBase {
    public static final int RECEIVERS = 2;

    public BroadcastBackPressureTest(BackPressureTestContext ctx) {
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
    public void testBroadcast() throws Exception {
        CountDownLatch resumeReceive = new CountDownLatch(1);

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.get().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        createChannel(c -> useBackPressure(c).withReceiver(msg -> {
            if (!msg.get().equals("init")) {
                await(resumeReceive);
            }
        })).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        BroadcastFuture<String> future = null;

        try {
            for (int step = 0; step < 2000000; step++) {
                future = sender.broadcast("message-" + step);

                if (future.isDone() && !future.get().isSuccess()) {
                    say("Completed after " + step + " steps.");

                    break;
                }
            }
        } finally {
            resumeReceive.countDown();
        }

        assertNotNull(future);

        Map<ClusterNode, Throwable> errors = future.get().getErrors();

        assertFalse(errors.isEmpty());
        assertTrue(errors.toString(), errors.values().stream().allMatch(e -> Utils.isCausedBy(e, MessagingOverflowException.class)));
    }

    @Test
    public void testAggregate() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests1.add(msg);
                }
            })
        ).join();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests2.add(msg);
                }
            })
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark / RECEIVERS; i++) {
            futureResponses.add(sender.aggregate("request-" + i));
        }

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        // Go down to low watermark.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests2.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        sender.broadcast("last").get(3, TimeUnit.SECONDS);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));
        requests2.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            future.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testAggregateFailure() throws Exception {
        List<Message<String>> requests1 = new CopyOnWriteArrayList<>();
        List<Message<String>> requests2 = new CopyOnWriteArrayList<>();

        createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests1.add(msg);
                }
            })
        ).join();

        TestChannel receiver2 = createChannel(c -> useBackPressure(c)
            .withReceiver(msg -> {
                if (!"init".equals(msg.get())) {
                    requests2.add(msg);
                }
            })
        ).join();

        MessagingChannel<String> sender = createChannel(this::useBackPressure).join().get().forRemotes();

        // Ensure that connection to each node is established.
        sender.broadcast("init").get(3, TimeUnit.SECONDS);

        // Request (aggregate) up to high watermark in order to trigger back pressure.
        List<AggregateFuture<?>> futureResponses = new ArrayList<>();

        for (int i = 0; i < highWatermark / RECEIVERS; i++) {
            futureResponses.add(sender.aggregate("request-" + i));
        }

        busyWait("requests received", () -> requests1.size() == futureResponses.size());
        busyWait("requests received", () -> requests2.size() == futureResponses.size());

        // Check that message can't be sent when high watermark reached.
        assertBackPressureEnabled(sender);

        // Go down to low watermark on first node.
        for (int i = 0; i < getLowWatermarkBounds(); i++) {
            String request = "request-" + i;

            requests1.stream().filter(r -> r.get().equals(request)).findFirst().ifPresent(r -> r.reply("ok"));
        }

        // Stop second receiver so that all pending requests would partially fail.
        receiver2.leave();

        busyWait("responses received", () ->
            futureResponses.stream().filter(CompletableFuture::isDone).count() == getLowWatermarkBounds()
        );

        // Check that new request can be processed.
        sender.broadcast("last").get(3, TimeUnit.SECONDS);

        requests1.stream().filter(Message::mustReply).forEach(r -> r.reply("ok"));

        for (Future<?> future : futureResponses) {
            future.get(3, TimeUnit.SECONDS);
        }
    }
}
