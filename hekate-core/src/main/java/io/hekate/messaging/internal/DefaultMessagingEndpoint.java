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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class DefaultMessagingEndpoint<T> implements MessagingEndpoint<T> {
    private final int poolOrder;

    private final int poolSize;

    @ToStringIgnore
    private final MessagingChannelId channelId;

    private final MessagingChannel<T> channel;

    private volatile Object userContext;

    public DefaultMessagingEndpoint(MessagingChannelId channelId, int poolOrder, int poolSize, MessagingChannel<T> channel) {
        assert channelId != null : "Channel ID is null.";
        assert channel != null : "Channel is null.";

        this.channelId = channelId;
        this.poolOrder = poolOrder;
        this.poolSize = poolSize;
        this.channel = channel;
    }

    @Override
    public MessagingChannelId getRemoteId() {
        return channelId;
    }

    @Override
    public int getSocketOrder() {
        return poolOrder;
    }

    @Override
    public int getSockets() {
        return poolSize;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C> C getContext() {
        return (C)userContext;
    }

    @Override
    public void setContext(Object ctx) {
        this.userContext = ctx;
    }

    @Override
    public MessagingChannel<T> getChannel() {
        return channel;
    }

    @Override
    public String toString() {
        return ToString.format(MessagingEndpoint.class, this);
    }
}
