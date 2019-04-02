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
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.operation.AggregateFuture;
import io.hekate.messaging.operation.AggregateResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MessagingChannelAggregateTest extends MessagingServiceTestBase {
    public MessagingChannelAggregateTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void testDefaultRouting() throws Exception {
        List<TestChannel> channels = createAndJoinChannels(5, c ->
            c.setReceiver(msg -> msg.reply(msg.channel().id().toString()))
        );

        Set<String> channelIds = channels.stream().map(c -> c.channel().id().toString()).collect(Collectors.toSet());

        for (TestChannel channel : channels) {
            AggregateResult<String> result = channel.channel().newAggregate("test").get();

            assertTrue(result.errors().toString(), result.isSuccess());
            assertEquals(channels.size(), result.results().size());

            Set<String> replies = new HashSet<>(result.results());

            assertEquals(channelIds, replies);
        }
    }

    @Test
    public void testAffinity() throws Exception {
        repeat(2, i -> {
            int nodesPerPartition = i + 1;

            List<TestChannel> channels = createAndJoinChannels(5, c -> {
                c.setPartitions(256);
                c.setBackupNodes(nodesPerPartition - 1);
                c.setReceiver(msg -> msg.reply(msg.channel().id().toString()));
            });

            for (TestChannel channel : channels) {
                repeat(100, j -> {
                    AggregateResult<String> result = channel.channel()
                        .newAggregate("test-" + j)
                        .withAffinity(j)
                        .get();

                    assertTrue(result.isSuccess());

                    List<ClusterNode> receivedBy = result.nodes();
                    List<ClusterNode> mappedTo = channel.channel().partitions().map(j).nodes();

                    assertEquals(nodesPerPartition, receivedBy.size());
                    assertEquals(mappedTo.stream().sorted().collect(toList()), receivedBy.stream().sorted().collect(toList()));
                });
            }

            for (TestChannel channel : channels) {
                channel.leave();
            }
        });
    }

    @Test
    public void testFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.payload() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            AggregateFuture<String> future = channel.channel().newAggregate("test" + i).submit();

            AggregateResult<String> result = get(future);

            assertSame(result.results(), future.results());

            assertSame(result.results(), future.results(String.class));

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.errors().isEmpty());
            assertEquals(channels.size(), result.nodes().size());
            assertEquals(channels.size(), result.results().size());
            assertEquals(new HashSet<>(result.results()), result.stream().collect(Collectors.toSet()));

            result.results().forEach(r -> assertEquals("test" + i + "-reply", r));
        });
    }

    @Test
    public void testEmptyTopologyFuture() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> msg.reply(msg.payload() + "-reply"))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            // Empty targets.
            AggregateFuture<String> future = channel.channel().forRole("no-such-role").newAggregate("test" + i).submit();

            AggregateResult<String> result = get(future);

            assertSame(result.results(), future.results());

            assertTrue(result.errors().toString(), result.isSuccess());
            assertTrue(result.errors().isEmpty());
            assertTrue(result.nodes().isEmpty());
            assertTrue(result.results().isEmpty());
            assertEquals("test" + i, result.request());
            assertEquals(new HashSet<>(result.results()), result.stream().collect(Collectors.toSet()));
            assertNull(result.errorOf(channel.node().localNode()));
            assertNull(result.resultOf(channel.node().localNode()));
            assertTrue(result.isSuccess(channel.node().localNode()));
            assertTrue(result.toString().startsWith(AggregateResult.class.getSimpleName()));
        });
    }

    @Test
    public void testPartialFailureFuture() throws Exception {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.payload() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(5, c -> c.setReceiver(receiver));

        repeat(channels.size() - 1, i -> {
            channels.get(i).impl().close().await();

            TestChannel channel = channels.get(channels.size() - 1);

            AggregateResult<String> result = channel.channel().newAggregate("test" + i).get();

            assertEquals("test" + i, result.request());
            assertFalse(result.isSuccess());
            assertEquals(result.errors().toString(), i + 1, result.errors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).node().localNode();

                assertNotNull(result.errors().get(node));
            }

            for (int j = i + 1; j < channels.size(); j++) {
                ClusterNode node = channels.get(j).node().localNode();

                assertNull(result.errors().get(node));
                assertEquals("test" + i + "-reply", result.resultsByNode().get(node));
            }
        });
    }

    @Test
    public void testTopologyChange() throws Throwable {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.payload() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(receiver));

        TestChannel channel = channels.get(0);

        repeat(3, i -> {
            say("Aggregate with on join topology change.");

            CountDownLatch joinFilterCallLatch = new CountDownLatch(1);
            CountDownLatch joinLatch = new CountDownLatch(1);

            Future<AggregateFuture<String>> joinAggregateFuture = runAsync(() ->
                channel.channel().filterAll(nodes -> {
                    joinFilterCallLatch.countDown();

                    await(joinLatch);

                    return nodes;
                }).newAggregate("test-join" + i).submit()
            );

            await(joinFilterCallLatch);

            TestChannel joined = createChannel(c -> c.setReceiver(receiver));

            channels.add(joined.join());

            awaitForChannelsTopology(channels);

            joinLatch.countDown();

            AggregateResult<String> joinResult = get(get(joinAggregateFuture));

            assertTrue(joinResult.isSuccess());

            for (TestChannel c : channels) {
                if (c == joined) {
                    assertFalse(joinResult.nodes().contains(joined.node().localNode()));
                } else {
                    assertTrue(joinResult.nodes().contains(c.node().localNode()));

                    c.awaitForMessage("test-join" + i);
                }
            }

            say("Aggregate with on leave topology change.");

            CountDownLatch leaveFilterCallLatch = new CountDownLatch(1);
            CountDownLatch leaveLatch = new CountDownLatch(1);

            Future<AggregateFuture<String>> leaveAggregateFuture = runAsync(() ->
                channel.channel().filterAll(nodes -> {
                    leaveFilterCallLatch.countDown();

                    await(leaveLatch);

                    return nodes;
                }).newAggregate("test-join" + i).submit()
            );

            await(leaveFilterCallLatch);

            TestChannel left = channels.remove(channels.size() - 1);

            ClusterNode leftNode = left.node().localNode();

            left.leave();

            awaitForChannelsTopology(channels);

            leaveLatch.countDown();

            AggregateResult<String> leaveResult = get(get(leaveAggregateFuture));

            assertTrue(leaveResult.errors().toString(), leaveResult.isSuccess());

            assertFalse(leaveResult.nodes().contains(leftNode));
        });
    }

    @Test
    public void testNonChannelNodes() throws Exception {
        MessageReceiver<String> receiver = msg -> msg.reply(msg.payload() + "-reply");

        List<TestChannel> channels = createAndJoinChannels(3, c -> c.setReceiver(receiver));

        List<HekateTestNode> allNodes = new ArrayList<>();

        // Nodes without channel.
        allNodes.add(createNode().join());
        allNodes.add(createNode().join());

        channels.forEach(c -> allNodes.add(c.node()));

        awaitForTopology(allNodes);

        for (TestChannel channel : channels) {
            AggregateResult<String> result = get(channel.channel().newAggregate("test").submit());

            assertEquals("test", result.request());
            assertTrue(result.isSuccess());
            assertEquals(0, result.errors().size());
            assertEquals(channels.size(), result.results().size());

            channels.forEach(c ->
                assertEquals("test-reply", result.resultsByNode().get(c.node().localNode()))
            );
        }
    }
}
