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

package io.hekate.messaging;

import io.hekate.core.service.ServiceFactory;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.messaging.internal.DefaultMessagingService;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link MessagingService}.
 */
public class MessagingServiceFactory implements ServiceFactory<MessagingService> {
    private List<MessagingChannelConfig<?>> channels;

    private List<MessageInterceptor> globalInterceptors;

    private List<MessagingConfigProvider> configProviders;

    /**
     * Returns the list of Channel configurations (see {@link #setChannels(List)}).
     *
     * @return List of channel configurations.
     */
    public List<MessagingChannelConfig<?>> getChannels() {
        return channels;
    }

    /**
     * Sets the list of channel configurations that should be registered to the {@link MessagingService}.
     *
     * @param channels Channel configurations.
     */
    public void setChannels(List<MessagingChannelConfig<?>> channels) {
        this.channels = channels;
    }

    /**
     * Fluent-style version of {@link #setChannels(List)}.
     *
     * @param channel Channel configuration that should be registered to the {@link MessagingService}.
     *
     * @return This instance.
     */
    public MessagingServiceFactory withChannel(MessagingChannelConfig<?> channel) {
        if (channels == null) {
            channels = new ArrayList<>();
        }

        channels.add(channel);

        return this;
    }

    /**
     * Returns the list of channel configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Channel configuration providers.
     */
    public List<MessagingConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of channel configuration providers.
     *
     * @param configProviders Channel configuration providers.
     *
     * @see MessagingConfigProvider
     */
    public void setConfigProviders(List<MessagingConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Channel configuration provider.
     *
     * @return This instance.
     */
    public MessagingServiceFactory withConfigProvider(MessagingConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    /**
     * Returns the list of message interceptors that should be applied to all channels (see {@link #setGlobalInterceptors(List)}).
     *
     * @return List of global message interceptors.
     */
    public List<MessageInterceptor> getGlobalInterceptors() {
        return globalInterceptors;
    }

    /**
     * Sets the list of message interceptors that should be applied to all channels.
     *
     * @param globalInterceptors List of global message interceptors.
     *
     * @see MessagingChannelConfig#setInterceptors(List)
     */
    public void setGlobalInterceptors(List<MessageInterceptor> globalInterceptors) {
        this.globalInterceptors = globalInterceptors;
    }

    /**
     * Fluent-style version of {@link #setGlobalInterceptors(List)}.
     *
     * @param globalInterceptor Message interceptor.
     *
     * @return This instance.
     */
    public MessagingServiceFactory withGlobalInterceptor(MessageInterceptor globalInterceptor) {
        if (getGlobalInterceptors() == null) {
            setGlobalInterceptors(new ArrayList<>());
        }

        getGlobalInterceptors().add(globalInterceptor);

        return this;
    }

    @Override
    public MessagingService createService() {
        return new DefaultMessagingService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
