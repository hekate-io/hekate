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

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingServiceFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class MessagingServiceTestBase extends HekateNodeParamTestBase {
    public interface ChannelConfigurer {
        void configure(MessagingChannelConfig<String> cfg);
    }

    public static class MessagingTestContext extends HekateTestContext {
        private final int workerThreads;

        private final int nioThreads;

        public MessagingTestContext(HekateTestContext parent, int workerThreads, int nioThreads) {
            super(parent);

            this.workerThreads = workerThreads;
            this.nioThreads = nioThreads;
        }
    }

    public static final String TEST_CHANNEL_NAME = "test-channel";

    public static final String TEST_NODE_ROLE = "test_comm";

    public static final int TEST_BACKOFF_DELAY = 10;

    protected final int workerThreads;

    protected final int nioThreads;

    public MessagingServiceTestBase(MessagingTestContext ctx) {
        super(ctx);

        this.workerThreads = ctx.workerThreads;
        this.nioThreads = ctx.nioThreads;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<MessagingTestContext> getMessagingServiceTestContexts() {
        return mapTestContext(p -> Stream.of(
            new MessagingTestContext(p, 0, 1),
            new MessagingTestContext(p, 1, 1),
            new MessagingTestContext(p, 4, 1),
            new MessagingTestContext(p, 5, 0)
        ));
    }

    public int workerThreads() {
        return workerThreads;
    }

    public int nioThreads() {
        return nioThreads;
    }

    protected List<TestChannel> createAndJoinChannels(int size) throws Exception {
        return createAndJoinChannels(size, null);
    }

    protected List<TestChannel> createAndJoinChannels(int size, ChannelConfigurer configurer) throws Exception {
        return createAndJoinChannels(size, configurer, null);
    }

    protected List<TestChannel> createAndJoinChannels(int size, ChannelConfigurer configurer, NodeConfigurer nodeConfigurer)
        throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            channels.add(createChannel(configurer, nodeConfigurer).join());
        }

        awaitForChannelsTopology(channels);

        return channels;
    }

    protected void awaitForChannelsTopology(TestChannel... channels) {
        awaitForChannelsTopology(Arrays.asList(channels));
    }

    protected void awaitForChannelsTopology(List<TestChannel> channels) {
        for (TestChannel c : channels) {
            c.awaitForTopology(channels);
        }
    }

    protected TestChannel createChannel() throws Exception {
        return createChannel(null);
    }

    protected TestChannel createChannel(ChannelConfigurer configurer) throws Exception {
        return createChannel(configurer, null);
    }

    protected TestChannel createChannel(ChannelConfigurer configurer, NodeConfigurer nodeConfigurer) throws Exception {
        MessagingChannelConfig<String> cfg = createChannelConfig();

        if (configurer != null) {
            configurer.configure(cfg);
        }

        TestChannel channel = new TestChannel(cfg.getReceiver());

        cfg.setReceiver(channel.receiver());

        HekateTestNode node = createNode(c -> {
            c.withRole(TEST_NODE_ROLE);

            c.withService(MessagingServiceFactory.class).withChannel(cfg);

            if (nodeConfigurer != null) {
                nodeConfigurer.configure(c);
            }
        });

        channel.initialize(node);

        return channel;
    }

    protected MessagingChannelConfig<String> createChannelConfig() {
        return MessagingChannelConfig.of(String.class)
            .withName(TEST_CHANNEL_NAME)
            .withWorkerThreads(workerThreads)
            .withNioThreads(nioThreads)
            .withRetryPolicy(retry ->
                retry.withFixedDelay(TEST_BACKOFF_DELAY)
            )
            .withClusterFilter(node ->
                node.hasRole(TEST_NODE_ROLE)
            )
            .withLogCategory(getClass().getName());
    }

    protected void assertResponded(Message<String> msg) {
        assertFalse(msg.mustReply());

        try {
            msg.reply("invalid");

            fail("Failure was expected.");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Message already responded"));
        }

        try {
            msg.reply("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Message already responded"));
        }

        if (msg.isSubscription()) {
            try {
                msg.partialReply("invalid", new SendCallbackMock());

                fail("Failure was expected.");
            } catch (IllegalStateException e) {
                assertTrue(e.getMessage().startsWith("Message already responded"));
            }
        }
    }

    protected void assertResponseUnsupported(Message<String> msg) {
        try {
            msg.reply("invalid");

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Reply is not supported by this message.", e.getMessage());
        }

        try {
            msg.reply("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Reply is not supported by this message.", e.getMessage());
        }

        try {
            msg.partialReply("invalid", new SendCallbackMock());

            fail("Failure was expected.");
        } catch (UnsupportedOperationException e) {
            assertEquals("Reply is not supported by this message.", e.getMessage());
        }
    }
}
