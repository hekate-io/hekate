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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HekateMessagingServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    static class MessagingTestConfig extends HekateTestConfigBase {
        @Component
        private static class InnerComponent {
            private final MessagingChannel<Object> channel;

            public InnerComponent(@NamedMessagingChannel("test3") MessagingChannel<Object> channel) {
                this.channel = channel;
            }
        }

        private static class InnerBean {
            @NamedMessagingChannel("test2")
            private MessagingChannel<Object> innerChannel;
        }

        @NamedMessagingChannel("test1")
        private MessagingChannel<Object> channel;

        @Bean
        public InnerBean innerBean() {
            return new InnerBean();
        }

        @Bean
        public MessagingChannel<Object> channel1(MessagingService messagingService) {
            return messagingService.channel("test1");
        }

        @Bean
        public MessagingChannel<Object> channel2(MessagingService messagingService) {
            return messagingService.channel("test2");
        }

        @Bean
        public MessagingChannelConfig<Object> channel1Config() {
            return new MessagingChannelConfig<>().withName("test1");
        }

        @Bean
        public MessagingChannelConfig<Object> channel2Config() {
            return new MessagingChannelConfig<>().withName("test2");
        }

        @Bean
        public MessagingChannelConfig<Object> channel3Config() {
            return new MessagingChannelConfig<>().withName("test3");
        }
    }

    @Test
    public void testChannels() {
        registerAndRefresh(MessagingTestConfig.class);

        assertNotNull(get("messagingService", MessagingService.class));

        assertNotNull(get(MessagingTestConfig.class).channel);

        assertNotNull(get(MessagingTestConfig.InnerBean.class).innerChannel);
        assertNotNull(get(MessagingTestConfig.InnerComponent.class).channel);

        assertEquals("test1", get(MessagingTestConfig.class).channel.getName());
        assertEquals("test2", get(MessagingTestConfig.InnerBean.class).innerChannel.getName());
        assertEquals("test3", get(MessagingTestConfig.InnerComponent.class).channel.getName());

        assertNotNull(getNode().get(MessagingService.class).channel("test1"));
        assertNotNull(getNode().get(MessagingService.class).channel("test2"));

        class TestAutowire {
            @Autowired
            private MessagingService messagingService;

            @Autowired
            @Qualifier("channel1")
            private MessagingChannel<Object> channel1;

            @Autowired
            @Qualifier("channel2")
            private MessagingChannel<Object> channel2;
        }

        assertNotNull(autowire(new TestAutowire()).channel1);
        assertNotNull(autowire(new TestAutowire()).channel2);
        assertNotNull(autowire(new TestAutowire()).messagingService);
    }
}
