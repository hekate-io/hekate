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
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.operation.BroadcastFuture;
import io.hekate.messaging.operation.BroadcastResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MessagingChannelBroadcastConfirmationTest extends MessagingServiceTestBase {
    public MessagingChannelBroadcastConfirmationTest(MessagingTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        repeat(5, i -> {
            TestChannel channel = createChannel(c -> c.setReceiver(msg -> assertFalse(msg.mustReply()))).join();

            channels.add(channel);

            awaitForChannelsTopology(channels);

            BroadcastFuture<String> future = channel.channel().newBroadcast("test" + i)
                .withAck()
                .submit();

            BroadcastResult<String> result = get(future);

            assertTrue(result.toString(), result.isSuccess());
            assertTrue(result.toString(), result.errors().isEmpty());
            assertEquals(channels.size(), result.nodes().size());
        });
    }

    @Test
    public void testAffinity() throws Exception {
        repeat(2, i -> {
            int nodesPerPartition = i + 1;

            List<TestChannel> channels = createAndJoinChannels(5, c -> {
                c.setPartitions(256);
                c.setBackupNodes(nodesPerPartition - 1);
                c.setReceiver(msg -> assertFalse(msg.mustReply()));
            });

            for (TestChannel channel : channels) {
                repeat(100, j -> {
                    BroadcastResult<String> result = channel.channel()
                        .newBroadcast("test-" + j)
                        .withAffinity(j)
                        .withAck()
                        .sync();

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
    public void testPartialFailureFuture() throws Exception {
        MessageReceiver<String> receiver = msg -> assertFalse(msg.mustReply());

        List<TestChannel> channels = createAndJoinChannels(5, c -> c.setReceiver(receiver));

        repeat(channels.size() - 1, i -> {
            channels.get(i).impl().close().await();

            TestChannel channel = channels.get(channels.size() - 1);

            BroadcastResult<String> result = channel.channel()
                .newBroadcast("test" + i)
                .withAck()
                .sync();

            assertEquals("test" + i, result.message());
            assertFalse(result.isSuccess());
            assertEquals(result.errors().toString(), i + 1, result.errors().size());

            for (int j = 0; j <= i; j++) {
                ClusterNode node = channels.get(j).node().localNode();

                assertNotNull(result.errors().get(node));
            }

            for (int j = i + 1; j < channels.size(); j++) {
                ClusterNode node = channels.get(j).node().localNode();

                assertNull(result.errors().get(node));
            }
        });
    }
}
