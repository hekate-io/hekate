package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateTestInstance;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.broadcast.AggregateCallback;
import io.hekate.messaging.broadcast.AggregateFuture;
import io.hekate.messaging.broadcast.AggregateResult;
import io.hekate.messaging.unicast.Response;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MessagingChannelAggregateTest extends MessagingServiceTestBase {
    private static class AggregateTestCallback extends CompletableFuture<AggregateResult<String>> implements AggregateCallback<String> {
        private final ConcurrentMap<ClusterNode, List<String>> replies = new ConcurrentHashMap<>();

        @Override
        public void onReplySuccess(Response<String> rsp, ClusterNode node) {
            List<String> nodeReplies = replies.get(node);

            if (nodeReplies == null) {
                nodeReplies = Collections.synchronizedList(new ArrayList<>());

                List<String> existing = replies.putIfAbsent(node, nodeReplies);

                if (existing != null) {
                    nodeReplies = existing;
                }
            }

            nodeReplies.add(rsp.get());
        }

        @Override
        public void onComplete(Throwable err, AggregateResult<String> result) {
            if (err == null) {
                complete(result);
            } else {
                completeExceptionally(err);
            }
        }
    }

    public MessagingChannelAggregateTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testDefaultRouting() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(5, c ->
            c.setReceiver(msg -> msg.reply(msg.getChannel().getId().toString()))
        );

        Set<String> channelIds = channels.stream().map(c -> c.getId().toString()).collect(Collectors.toSet());

        for (TestChannel channel : channels) {
            AggregateResult<String> result = channel.get().aggregate("test").get();

            assertTrue(result.errors().toString(), result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            Set<String> replies = new HashSet<>(result.results());

            assertEquals(channelIds, replies);
        }
    }

    @Test
    public void testCallback() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.get() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            AggregateTestCallback callback = new AggregateTestCallback();

            channel.get().aggregate("test" + i, callback);

            AggregateResult<String> result = callback.get();

            assertTrue(result.errors().toString(), result.isSuccess());
            assertTrue(result.errors().isEmpty());
            assertEquals(channels.size(), result.nodes().size());
            assertEquals(channels.size(), result.results().size());
            assertEquals(new HashSet<>(result.results()), result.stream().collect(Collectors.toSet()));

            result.results().forEach(r -> assertEquals("test" + i + "-reply", r));
        });
    }

    @Test
    public void testFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.get() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            AggregateFuture<String> future = channel.get().aggregate("test" + i);

            AggregateResult<String> result = get(future);

            assertSame(result.results(), future.results());
            assertSame(result.results(), future.results(3, TimeUnit.SECONDS));
            assertSame(result.results(), future.resultsUninterruptedly());

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.errors().isEmpty());
            assertEquals(channels.size(), result.nodes().size());
            assertEquals(channels.size(), result.results().size());
            assertEquals(new HashSet<>(result.results()), result.stream().collect(Collectors.toSet()));

            result.results().forEach(r -> assertEquals("test" + i + "-reply", r));
        });
    }

    @Test
    public void testEmptyRouteFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.get() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            // Empty targets.
            AggregateFuture<String> future = channel.get().forRole("no-such-role").aggregate("test" + i);

            AggregateResult<String> result = get(future);

            assertSame(result.results(), future.results());
            assertSame(result.results(), future.results(3, TimeUnit.SECONDS));
            assertSame(result.results(), future.resultsUninterruptedly());

            assertTrue(result.errors().toString(), result.isSuccess());
            assertTrue(result.errors().isEmpty());
            assertTrue(result.nodes().isEmpty());
            assertTrue(result.results().isEmpty());
            assertEquals("test" + i, result.request());
            assertEquals(new HashSet<>(result.results()), result.stream().collect(Collectors.toSet()));
            assertNull(result.errorOf(channel.getInstance().getNode()));
            assertNull(result.resultOf(channel.getInstance().getNode()));
            assertFalse(result.isSuccess(channel.getInstance().getNode()));
            assertTrue(result.toString().startsWith(AggregateResult.class.getSimpleName()));
        });
    }

    @Test
    public void testEmptyRouteCallback() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.get() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            // Empty targets.
            AggregateTestCallback callback = new AggregateTestCallback();

            channel.get().forRole("no-such-role").aggregate("test" + i, callback);

            AggregateResult<String> result = get(callback);

            assertTrue(result.errors().toString(), result.isSuccess());
            assertTrue(result.errors().isEmpty());
            assertTrue(result.nodes().isEmpty());
            assertTrue(result.results().isEmpty());
        });
    }

    @Test
    public void testPartialFailureCallback() throws Exception {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(5, c -> c.setReceiver(receiver));

        repeat(channels.size() - 1, i -> {
            channels.get(i).getImpl().close().await();

            TestChannel channel = channels.get(channels.size() - 1);

            AggregateTestCallback callback = new AggregateTestCallback();

            channel.get().aggregate("test" + i, callback);

            AggregateResult<String> result = get(callback);

            assertEquals("test" + i, result.request());
            assertFalse(result.isSuccess());
            assertEquals(result.errors().toString(), i + 1, result.errors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNotNull(result.errors().get(node));
                assertSame(result.errors().get(node), result.errorOf(node));
                assertFalse(result.isSuccess(node));
            }

            for (int j = i + 1; j < channels.size(); j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNull(result.errors().get(node));
                assertEquals("test" + i + "-reply", result.resultsByNode().get(node));
                assertEquals("test" + i + "-reply", result.resultOf(node));
            }
        });
    }

    @Test
    public void testPartialFailureFuture() throws Exception {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(5, c -> c.setReceiver(receiver));

        repeat(channels.size() - 1, i -> {
            channels.get(i).getImpl().close().await();

            TestChannel channel = channels.get(channels.size() - 1);

            AggregateResult<String> result = get(channel.get().aggregate("test" + i));

            assertEquals("test" + i, result.request());
            assertFalse(result.isSuccess());
            assertEquals(result.errors().toString(), i + 1, result.errors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNotNull(result.errors().get(node));
            }

            for (int j = i + 1; j < channels.size(); j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNull(result.errors().get(node));
                assertEquals("test" + i + "-reply", result.resultsByNode().get(node));
            }
        });
    }

    @Test
    public void testTopologyChange() throws Throwable {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(receiver));

        TestChannel channel = channels.get(0);

        repeat(3, i -> {
            say("Aggregate with on join topology change.");

            CountDownLatch joinFilterCallLatch = new CountDownLatch(1);
            CountDownLatch joinLatch = new CountDownLatch(1);

            AggregateTestCallback joinCallback = new AggregateTestCallback();

            runAsync(() -> {
                channel.get().filterAll(nodes -> {
                    joinFilterCallLatch.countDown();

                    await(joinLatch);

                    return new HashSet<>(nodes);
                }).aggregate("test-join" + i, joinCallback);

                return null;
            });

            await(joinFilterCallLatch);

            TestChannel joined = createChannel(c -> c.setReceiver(receiver));

            channels.add(joined.join());

            awaitForChannelsTopology(channels);

            joinLatch.countDown();

            AggregateResult<String> joinResult = get(joinCallback);

            assertTrue(joinResult.isSuccess());

            for (TestChannel c : channels) {
                if (c == joined) {
                    assertFalse(joinResult.nodes().contains(joined.getInstance().getNode()));
                } else {
                    assertTrue(joinResult.nodes().contains(c.getInstance().getNode()));

                    c.awaitForMessage("test-join" + i);
                }
            }

            say("Aggregate with on leave topology change.");

            CountDownLatch leaveFilterCallLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);

            AggregateTestCallback leaveCallback = new AggregateTestCallback();

            runAsync(() -> {
                channel.get().filterAll(nodes -> {
                    leaveFilterCallLatch.countDown();

                    await(leaveLatch);

                    return new HashSet<>(nodes);
                }).aggregate("test-join" + i, leaveCallback);

                return null;
            });

            await(leaveFilterCallLatch);

            TestChannel left = channels.remove(channels.size() - 1);

            ClusterNode leftNode = left.getInstance().getNode();

            left.leave();

            awaitForChannelsTopology(channels);

            leaveLatch.countDown();

            AggregateResult<String> leaveResult = get(leaveCallback);

            assertTrue(leaveResult.errors().toString(), leaveResult.isSuccess());

            assertFalse(leaveResult.nodes().contains(leftNode));
        });
    }

    @Test
    public void testNonChannelNodes() throws Exception {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.get() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(receiver));

        List<HekateTestInstance> allNodes = new ArrayList<>();

        // Nodes without channel.
        allNodes.add(createInstance().join());
        allNodes.add(createInstance().join());

        channels.forEach(c -> allNodes.add(c.getInstance()));

        awaitForTopology(allNodes);

        for (TestChannel channel : channels) {
            AggregateTestCallback callback = new AggregateTestCallback();

            channel.get().aggregate("test", callback);

            AggregateResult<String> result = get(callback);

            assertEquals("test", result.request());
            assertTrue(result.isSuccess());
            assertEquals(0, result.errors().size());
            assertEquals(channels.size(), result.results().size());

            channels.forEach(c ->
                assertEquals("test-reply", result.resultsByNode().get(c.getInstance().getNode()))
            );
        }
    }
}
