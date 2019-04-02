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

import io.hekate.messaging.MessagingChannel;
import io.hekate.network.netty.NettySpyForTest;
import io.hekate.network.netty.NetworkServiceFactoryForTest;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class RetryTestBase extends MessagingServiceTestBase {
    public static final String RETRANSMIT_SUFFIX = "retransmit";

    protected final AtomicInteger failures = new AtomicInteger();

    protected TestChannel sender;

    protected TestChannel receiver;

    protected MessagingChannel<String> toSelf;

    protected MessagingChannel<String> toRemote;

    protected NettySpyForTest spy;

    public RetryTestBase(MessagingTestContext ctx) {
        super(ctx);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        List<TestChannel> channels = createAndJoinChannels(2,
            channel -> channel.withReceiver(msg -> {
                if (spy == null && failures.getAndDecrement() > 0) {
                    throw TEST_ERROR;
                }

                if (msg.mustReply()) {
                    msg.reply(msg.payload() + "-" + (msg.isRetransmit() ? RETRANSMIT_SUFFIX : "invalid"));
                }
            }),
            boot -> boot.withService(NetworkServiceFactoryForTest.class, net -> {
                net.setSpy(spy);
            })
        );

        awaitForChannelsTopology(channels);

        sender = channels.get(0);

        receiver = channels.get(1);

        toSelf = sender.channel().forNode(sender.nodeId());
        toRemote = sender.channel().forNode(receiver.nodeId());
    }
}
