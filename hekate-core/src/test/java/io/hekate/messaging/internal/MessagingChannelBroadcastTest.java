package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.broadcast.BroadcastCallback;
import io.hekate.messaging.broadcast.BroadcastResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MessagingChannelBroadcastTest extends MessagingServiceTestBase {
    private static class BroadcastTestCallback extends CompletableFuture<BroadcastResult<String>> implements BroadcastCallback<String> {
        @Override
        public void onComplete(Throwable err, BroadcastResult<String> result) {
            if (err == null) {
                complete(result);
            } else {
                completeExceptionally(err);
            }
        }
    }

    public MessagingChannelBroadcastTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testCallback() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            BroadcastTestCallback callback = new BroadcastTestCallback();

            channel.get().broadcast("test" + i, callback);

            BroadcastResult<String> result = callback.get();

            assertTrue(result.getErrors().toString(), result.isSuccess());
            assertTrue(result.getErrors().isEmpty());
            assertEquals(channels.size(), result.getNodes().size());

            for (TestChannel c : channels) {
                c.awaitForMessage("test" + i);
            }
        });
    }

    @Test
    public void testFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            BroadcastResult<String> result = get(channel.get().broadcast("test" + i));

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.getErrors().isEmpty());
            assertEquals(channels.size(), result.getNodes().size());

            for (TestChannel c : channels) {
                c.awaitForMessage("test" + i);
            }
        });
    }

    @Test
    public void testEmptyRouteFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            // Empty targets.
            BroadcastResult<String> result = get(channel.get().forRole("no-such-role").broadcast("test" + i));

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.getErrors().isEmpty());
            assertTrue(result.toString(), result.getNodes().isEmpty());
            assertEquals("test" + i, result.getMessage());
            assertNull(result.getError(channel.getInstance().getNode()));
            assertFalse(result.isSuccess(channel.getInstance().getNode()));
            assertTrue(result.toString().startsWith(BroadcastResult.class.getSimpleName()));
        });
    }

    @Test
    public void testEmptyRouteCallback() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            // Empty targets.
            BroadcastTestCallback callback = new BroadcastTestCallback();

            channel.get().forRole("no-such-role").broadcast("test" + i, callback);

            BroadcastResult<String> result = get(callback);

            assertTrue(result.getErrors().toString(), result.isSuccess());
            assertTrue(result.getErrors().isEmpty());
            assertTrue(result.getNodes().isEmpty());
        });
    }

    @Test
    public void testPartialFailureCallback() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(5);

        TestChannel source = channels.get(channels.size() - 1);

        // Initialize connection to all nodes.
        get(source.get().broadcast("test"));

        repeat(channels.size() - 1, i -> {
            TestChannel target = channels.get(i);

            NetworkMessagingClient<String> client = (NetworkMessagingClient<String>)source.getImpl().getClient(target.getNodeId());

            // Induce failure by closing existing network connections.
            get(client.getConnection().disconnect());

            BroadcastTestCallback callback = new BroadcastTestCallback();

            source.get().broadcast("test" + i, callback);

            BroadcastResult<String> result = get(callback);

            assertEquals("test" + i, result.getMessage());
            assertFalse(result.getErrors().toString(), result.isSuccess());
            assertEquals(i + 1, result.getErrors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNotNull(result.getErrors().get(node));
            }
        });
    }

    @Test
    public void testPartialFailureFuture() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(5);

        TestChannel source = channels.get(channels.size() - 1);

        // Initialize connection to all nodes.
        get(source.get().broadcast("test"));

        repeat(channels.size() - 1, i -> {
            TestChannel target = channels.get(i);

            NetworkMessagingClient<String> client = (NetworkMessagingClient<String>)source.getImpl().getClient(target.getNodeId());

            // Induce failure by closing existing network connections.
            get(client.getConnection().disconnect());

            BroadcastResult<String> result = get(source.get().broadcast("test" + i));

            assertEquals("test" + i, result.getMessage());
            assertFalse(result.getErrors().toString(), result.isSuccess());
            assertEquals(i + 1, result.getErrors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).getInstance().getNode();

                assertNotNull(result.getErrors().get(node));
                assertSame(result.getErrors().get(node), result.getError(node));
                assertFalse(result.isSuccess(node));
            }
        });
    }

    @Test
    public void testTopologyChange() throws Throwable {
        List<TestChannel> channels = createAndJoinChannels(3);

        TestChannel channel = channels.get(0);

        repeat(3, i -> {
            say("Broadcast with on join topology change.");

            CountDownLatch joinFilterCallLatch = new CountDownLatch(1);
            CountDownLatch joinLatch = new CountDownLatch(1);

            BroadcastTestCallback joinCallback = new BroadcastTestCallback();

            runAsync(() -> {
                channel.get().filterAll(nodes -> {
                    joinFilterCallLatch.countDown();

                    await(joinLatch);

                    return new HashSet<>(nodes);
                }).broadcast("test-join" + i, joinCallback);

                return null;
            });

            await(joinFilterCallLatch);

            TestChannel joined = createChannel().join();

            channels.add(joined);

            awaitForChannelsTopology(channels);

            joinLatch.countDown();

            BroadcastResult<String> joinResult = get(joinCallback);

            assertTrue(joinResult.getErrors().toString(), joinResult.isSuccess());

            for (TestChannel c : channels) {
                if (c == joined) {
                    assertFalse(joinResult.getNodes().contains(joined.getInstance().getNode()));
                } else {
                    assertTrue(joinResult.getNodes().contains(c.getInstance().getNode()));

                    c.awaitForMessage("test-join" + i);
                }
            }

            say("Broadcast with on leave topology change.");

            CountDownLatch leaveFilterCallLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);

            BroadcastTestCallback leaveCallback = new BroadcastTestCallback();

            runAsync(() -> {
                channel.get().filterAll(nodes -> {
                    leaveFilterCallLatch.countDown();

                    await(leaveLatch);

                    return new HashSet<>(nodes);
                }).broadcast("test-join" + i, leaveCallback);

                return null;
            });

            await(leaveFilterCallLatch);

            TestChannel left = channels.remove(channels.size() - 1);

            ClusterNode leftNode = left.getInstance().getNode();

            left.leave();

            awaitForChannelsTopology(channels);

            leaveLatch.countDown();

            BroadcastResult<String> leaveResult = get(leaveCallback);

            assertTrue(leaveResult.getErrors().toString(), leaveResult.isSuccess());

            assertFalse(leaveResult.getNodes().contains(leftNode));

            for (TestChannel c : channels) {
                c.awaitForMessage("test-join" + i);
            }
        });
    }
}
