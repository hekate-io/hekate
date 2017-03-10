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

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.core.HekateTestInstance;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingServiceFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import org.junit.runners.Parameterized.Parameters;

public abstract class MessagingServiceTestBase extends HekateInstanceContextTestBase {
    public interface ChannelConfigurer {
        void configure(MessagingChannelConfig<String> cfg);
    }

    public static class MessagingTestContext extends HekateTestContext {
        private final int workerThreads;

        private final int sockets;

        private final int nioThreads;

        public MessagingTestContext(HekateTestContext parent, int workerThreads, int sockets, int nioThreads) {
            super(parent);

            this.workerThreads = workerThreads;
            this.sockets = sockets;
            this.nioThreads = nioThreads;
        }
    }

    public static final String TEST_CHANNEL_NAME = "test_channel";

    public static final String TEST_NODE_ROLE = "test_comm";

    private final int workerThreads;

    private final int sockets;

    private final int nioThreads;

    public MessagingServiceTestBase(MessagingTestContext ctx) {
        super(ctx);

        this.workerThreads = ctx.workerThreads;
        this.sockets = ctx.sockets;
        this.nioThreads = ctx.nioThreads;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<MessagingTestContext> getMessagingServiceTestContexts() {
        return mapTestContext(p -> Stream.of(
            new MessagingTestContext(p, 0, 1, 2),
            new MessagingTestContext(p, 1, 1, 1),
            new MessagingTestContext(p, 2, 2, 2),
            new MessagingTestContext(p, 3, 3, 3),
            new MessagingTestContext(p, 5, 2, 0),
            new MessagingTestContext(p, 2, 5, 0)
        ));
    }

    public int getWorkerThreads() {
        return workerThreads;
    }

    public int getSockets() {
        return sockets;
    }

    public int getNioThreads() {
        return nioThreads;
    }

    protected List<TestChannel> createAndJoinChannels(int size) throws Exception {
        return createAndJoinChannels(size, null);
    }

    protected List<TestChannel> createAndJoinChannels(int size, ChannelConfigurer configurer) throws Exception {
        List<TestChannel> channels = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            channels.add(createChannel(configurer).join());
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

    protected TestChannel createChannel(ChannelConfigurer configurer, InstanceConfigurer instanceConfigurer) throws Exception {
        MessagingChannelConfig<String> cfg = createChannelConfig();

        if (configurer != null) {
            configurer.configure(cfg);
        }

        TestChannel channel = new TestChannel(cfg.getReceiver());

        cfg.setReceiver(channel.getReceiver());

        HekateTestInstance instance = createInstance(c -> {
            c.withNodeRole(TEST_NODE_ROLE);

            c.findOrRegister(MessagingServiceFactory.class).withChannel(cfg);

            if (instanceConfigurer != null) {
                instanceConfigurer.configure(c);
            }
        });

        channel.initialize(instance);

        return channel;
    }

    protected MessagingChannelConfig<String> createChannelConfig() {
        MessagingChannelConfig<String> cfg = new MessagingChannelConfig<>();

        cfg.setName(TEST_CHANNEL_NAME);
        cfg.setWorkerThreads(workerThreads);
        cfg.setSockets(sockets);
        cfg.setNioThreads(nioThreads);
        cfg.setClusterFilter(node -> node.hasRole(TEST_NODE_ROLE));
        cfg.setLogCategory(getClass().getName());

        return cfg;
    }

    protected int extractAffinityKey(String message) {
        return Integer.parseInt(message.substring(0, message.indexOf(':')));
    }
}
