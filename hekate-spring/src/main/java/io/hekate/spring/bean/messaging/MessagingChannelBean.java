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

package io.hekate.spring.bean.messaging;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Imports {@link MessagingChannel} into a Spring context.
 */
public class MessagingChannelBean extends HekateBaseBean<MessagingChannel<?>> {
    private String channel;

    @Override
    public MessagingChannel<?> getObject() throws Exception {
        return getSource().messaging().channel(getChannel());
    }

    @Override
    public Class<MessagingChannel> getObjectType() {
        return MessagingChannel.class;
    }

    /**
     * Returns the channel name (see {@link #setChannel(String)}).
     *
     * @return Channel name.
     */
    public String getChannel() {
        return channel;
    }

    /**
     * Sets the channel name.
     *
     * @param channel Channel name.
     *
     * @see MessagingChannelConfig#setName(String)
     */
    @Required
    public void setChannel(String channel) {
        this.channel = channel;
    }
}
