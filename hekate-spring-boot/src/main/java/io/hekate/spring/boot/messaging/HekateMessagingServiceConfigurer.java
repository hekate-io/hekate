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

package io.hekate.spring.boot.messaging;

import io.hekate.core.Hekate;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.spring.bean.messaging.MessagingChannelBean;
import io.hekate.spring.bean.messaging.MessagingServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.spring.boot.internal.AnnotationInjectorBase;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link MessagingService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link MessagingServiceFactory} type and automatically {@link
 * MessagingServiceFactory#setChannels(List) registers} all {@link Bean}s of {@link MessagingChannelConfig} type.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * MessagingServiceFactory} type and if there is at least one {@link Bean} of {@link MessagingChannelConfig} type within the application
 * context.
 * </p>
 *
 * <h2>Channels injections</h2>
 * <p>
 * This auto-configuration provides support for injecting beans of {@link MessagingChannel} type into other beans with the help of {@link
 * InjectChannel} annotation. Please see its documentation for more details.
 * </p>
 *
 * @see MessagingService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnBean(MessagingChannelConfig.class)
@ConditionalOnMissingBean(MessagingServiceFactory.class)
public class HekateMessagingServiceConfigurer {
    @Component
    static class NamedMessagingChannelInjector extends AnnotationInjectorBase<InjectChannel> {
        public NamedMessagingChannelInjector() {
            super(InjectChannel.class, MessagingChannelBean.class);
        }

        @Override
        protected String injectedBeanName(InjectChannel annotation) {
            return MessagingChannelBean.class.getName() + "-" + annotation.value();
        }

        @Override
        protected Object qualifierValue(InjectChannel annotation) {
            return annotation.value();
        }

        @Override
        protected void configure(BeanDefinitionBuilder builder, InjectChannel annotation) {
            builder.addPropertyValue("channel", annotation.value());
        }
    }

    private final List<MessagingChannelConfig<?>> channels;

    /**
     * Constructs new instance.
     *
     * @param channels {@link MessagingChannelConfig}s that were found in the application context.
     */
    @Autowired
    public HekateMessagingServiceConfigurer(Optional<List<MessagingChannelConfig<?>>> channels) {
        this.channels = channels.orElse(null);
    }

    /**
     * Constructs the {@link MessagingServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    public MessagingServiceFactory messagingServiceFactory() {
        MessagingServiceFactory factory = new MessagingServiceFactory();

        factory.setChannels(channels);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link MessagingService} directly into other beans instead of accessing it
     * via {@link Hekate#messaging()} method.
     *
     * @return Service bean.
     */
    @Bean
    public MessagingServiceBean messagingService() {
        return new MessagingServiceBean();
    }
}
