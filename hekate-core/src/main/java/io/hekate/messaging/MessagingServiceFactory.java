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

package io.hekate.messaging;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.messaging.internal.DefaultMessagingService;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link MessagingService}.
 *
 * <p>
 * This class represents a configurable factory for {@link MessagingService}. Instances of this class must be {@link
 * HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to make {@link MessagingService}
 * accessible via {@link Hekate#get(Class)} method.
 * </p>
 *
 * <p>
 * For more details about the {@link MessagingService} and its capabilities please see the documentation of {@link MessagingService}
 * interface.
 * </p>
 */
public class MessagingServiceFactory implements ServiceFactory<MessagingService> {
    private List<MessagingChannelConfig<?>> channels;

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
     * Fluent-style shortcut to register a new {@link MessagingChannelConfig} with the specified
     * {@link MessagingChannelConfig#setName(String) name}.
     *
     * @param name Channel name (see {@link MessagingChannelConfig#setName(String)}).
     * @param <T> Base class of messages that can be handled by the channel.
     *
     * @return New channel configuration.
     */
    public <T> MessagingChannelConfig<T> withChannel(String name) {
        MessagingChannelConfig<T> channel = new MessagingChannelConfig<>(name);

        withChannel(channel);

        return channel;
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

    @Override
    public MessagingService createService() {
        return new DefaultMessagingService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
