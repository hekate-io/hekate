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
import io.hekate.messaging.operation.BroadcastCallback;
import io.hekate.messaging.operation.BroadcastResult;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
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
    public void testBroadcast() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            BroadcastResult<String> result = channel.channel().newBroadcast("test" + i).sync();

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.errors().isEmpty());
            assertEquals(channels.size(), result.nodes().size());

            for (TestChannel c : channels) {
                c.awaitForMessage("test" + i);
            }
        });
    }

    @Test
    public void testAffinity() throws Exception {
        repeat(2, i -> {
            int nodesPerPartition = i + 1;

            List<TestChannel> channels = createAndJoinChannels(5, c -> {
                c.setPartitions(256);
                c.setBackupNodes(nodesPerPartition - 1);
                c.setReceiver(msg -> { /* ignore */ });
            });

            for (TestChannel channel : channels) {
                repeat(100, j -> {
                    BroadcastResult<String> result = channel.channel()
                        .newBroadcast("test-" + j)
                        .withAffinity(j)
                        .sync();

                    assertTrue(result.isSuccess());

                    List<ClusterNode> sentTo = result.nodes();
                    List<ClusterNode> mappedTo = channel.channel().partitions().map(j).nodes();

                    assertEquals(nodesPerPartition, sentTo.size());
                    assertEquals(mappedTo.stream().sorted().collect(toList()), sentTo.stream().sorted().collect(toList()));
                });
            }

            for (TestChannel channel : channels) {
                channel.leave();
            }
        });
    }

    @Test
    public void testEmptyTopology() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel().join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            BroadcastResult<String> result = channel.channel()
                .forRole("no-such-role") // Empty targets.
                .newBroadcast("test" + i)
                .sync();

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.errors().isEmpty());
            assertTrue(result.toString(), result.nodes().isEmpty());
            assertEquals("test" + i, result.message());
            assertNull(result.errorOf(channel.node().localNode()));
            assertFalse(result.isSuccess(channel.node().localNode()));
            assertTrue(result.toString().startsWith(BroadcastResult.class.getSimpleName()));
        });
    }

    @Test
    public void testPartialFailure() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(5);

        TestChannel source = channels.get(channels.size() - 1);

        // Initialize connection to all nodes.
        source.channel().newBroadcast("test").sync();

        repeat(channels.size() - 1, i -> {
            TestChannel target = channels.get(i);

            MessagingClient<String> client = source.impl().clientOf(target.nodeId());

            // Induce failure by closing existing network connections.
            client.close();

            BroadcastResult<String> result = get(source.channel().newBroadcast("test" + i).submit());

            assertEquals("test" + i, result.message());
            assertFalse(result.errors().toString(), result.isSuccess());
            assertEquals(i + 1, result.errors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).node().localNode();

                assertNotNull(result.errors().get(node));
                assertSame(result.errors().get(node), result.errorOf(node));
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
                channel.channel().filterAll(nodes -> {
                    joinFilterCallLatch.countDown();

                    await(joinLatch);

                    return nodes;
                }).newBroadcast("test-join" + i).submit(joinCallback);

                return null;
            });

            await(joinFilterCallLatch);

            TestChannel joined = createChannel().join();

            channels.add(joined);

            awaitForChannelsTopology(channels);

            joinLatch.countDown();

            BroadcastResult<String> joinResult = get(joinCallback);

            assertTrue(joinResult.errors().toString(), joinResult.isSuccess());

            for (TestChannel c : channels) {
                if (c == joined) {
                    assertFalse(joinResult.nodes().contains(joined.node().localNode()));
                } else {
                    assertTrue(joinResult.nodes().contains(c.node().localNode()));

                    c.awaitForMessage("test-join" + i);
                }
            }

            say("Broadcast with on leave topology change.");

            CountDownLatch leaveFilterCallLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);

            BroadcastTestCallback leaveCallback = new BroadcastTestCallback();

            runAsync(() -> {
                channel.channel().filterAll(nodes -> {
                    leaveFilterCallLatch.countDown();

                    await(leaveLatch);

                    return nodes;
                }).newBroadcast("test-join" + i).submit(leaveCallback);

                return null;
            });

            await(leaveFilterCallLatch);

            TestChannel left = channels.remove(channels.size() - 1);

            ClusterNode leftNode = left.node().localNode();

            left.leave();

            awaitForChannelsTopology(channels);

            leaveLatch.countDown();

            BroadcastResult<String> leaveResult = get(leaveCallback);

            assertTrue(leaveResult.errors().toString(), leaveResult.isSuccess());

            assertFalse(leaveResult.nodes().contains(leftNode));

            for (TestChannel c : channels) {
                c.awaitForMessage("test-join" + i);
            }
        });
    }
}
