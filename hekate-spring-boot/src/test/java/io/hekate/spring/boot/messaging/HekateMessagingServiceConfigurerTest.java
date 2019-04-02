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

package io.hekate.spring.boot.messaging;

import io.hekate.core.Hekate;
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
import static org.junit.Assert.assertTrue;

public class HekateMessagingServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    static class TypeA<T> {
        @SuppressWarnings("unused")
        private T t;
    }

    static class TypeB<T> {
        @SuppressWarnings("unused")
        private T t;
    }

    @EnableAutoConfiguration
    public static class MessagingTestConfig extends HekateTestConfigBase {
        @Component
        private static class InnerComponent {
            private final MessagingChannel<Object> channel3;

            private MessagingChannel<Object> channel4;

            private MessagingChannel<Object> channel5;

            public InnerComponent(@InjectChannel("test3") MessagingChannel<Object> channel3) {
                this.channel3 = channel3;
            }

            @InjectChannel("test4")
            public void setChannel4(MessagingChannel<Object> channel4) {
                this.channel4 = channel4;
            }

            @Autowired
            public void useChannel5(@InjectChannel("test5") MessagingChannel<Object> channel5) {
                this.channel5 = channel5;
            }
        }

        private static class InnerBean {
            @InjectChannel("test2")
            private MessagingChannel<Object> innerChannel;
        }

        @InjectChannel("test1")
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
            return MessagingChannelConfig.unchecked().withName("test1");
        }

        @Bean
        public MessagingChannelConfig<Object> channel2Config() {
            return MessagingChannelConfig.unchecked().withName("test2");
        }

        @Bean
        public MessagingChannelConfig<Object> channel3Config() {
            return MessagingChannelConfig.unchecked().withName("test3");
        }

        @Bean
        public MessagingChannelConfig<Object> channel4Config() {
            return MessagingChannelConfig.unchecked().withName("test4");
        }

        @Bean
        public MessagingChannelConfig<Object> channel5Config() {
            return MessagingChannelConfig.unchecked().withName("test5");
        }
    }

    @EnableAutoConfiguration
    public static class MessagingTypeSafetyTestConfig extends HekateTestConfigBase {
        @Component
        private static class InnerComponent {
            private final MessagingChannel<String> channel1;

            private final MessagingChannel<TypeA<?>> channel2;

            private final MessagingChannel<TypeA<TypeB<?>>> channel3;

            private final MessagingChannel<TypeA<TypeB<String>>> channel4;

            public InnerComponent(
                @InjectChannel("test1") MessagingChannel<String> channel1,
                @InjectChannel("test2") MessagingChannel<TypeA<?>> channel2,
                @InjectChannel("test2") MessagingChannel<TypeA<TypeB<?>>> channel3,
                @InjectChannel("test2") MessagingChannel<TypeA<TypeB<String>>> channel4
            ) {
                this.channel1 = channel1;
                this.channel2 = channel2;
                this.channel3 = channel3;
                this.channel4 = channel4;
            }
        }

        @Bean
        public MessagingChannelConfig<String> channel1Config() {
            return MessagingChannelConfig.of(String.class).withName("test1");
        }

        @Bean
        public MessagingChannelConfig<TypeA> channel2Config() {
            return MessagingChannelConfig.of(TypeA.class).withName("test2");
        }
    }

    @EnableAutoConfiguration
    public static class CustomizeChannelConfig extends HekateTestConfigBase {
        @Bean
        public MessagingChannel<String> testChannel(Hekate hekate) {
            return hekate.messaging().channel("test", String.class).forRemotes();
        }

        @Bean
        public MessagingChannelConfig<String> testChannelConfig() {
            return MessagingChannelConfig.of(String.class).withName("test");
        }
    }

    @Test
    public void testChannels() {
        registerAndRefresh(MessagingTestConfig.class);

        assertNotNull(get("messagingService", MessagingService.class));

        assertNotNull(get(MessagingTestConfig.class).channel);

        assertNotNull(get(MessagingTestConfig.InnerBean.class).innerChannel);
        assertNotNull(get(MessagingTestConfig.InnerComponent.class).channel3);

        assertEquals("test1", get(MessagingTestConfig.class).channel.name());
        assertEquals("test2", get(MessagingTestConfig.InnerBean.class).innerChannel.name());
        assertEquals("test3", get(MessagingTestConfig.InnerComponent.class).channel3.name());
        assertEquals("test4", get(MessagingTestConfig.InnerComponent.class).channel4.name());
        assertEquals("test5", get(MessagingTestConfig.InnerComponent.class).channel5.name());

        assertNotNull(getNode().messaging().channel("test1"));
        assertNotNull(getNode().messaging().channel("test2"));

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

    @Test
    public void testChannelTypeSafety() {
        registerAndRefresh(MessagingTypeSafetyTestConfig.class);

        assertEquals("test1", get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel1.name());
        assertEquals(String.class, get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel1.baseType());

        assertEquals("test2", get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel2.name());
        assertEquals(TypeA.class, get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel2.baseType());

        assertEquals("test2", get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel3.name());
        assertEquals(TypeA.class, get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel3.baseType());

        assertEquals("test2", get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel4.name());
        assertEquals(TypeA.class, get(MessagingTypeSafetyTestConfig.InnerComponent.class).channel4.baseType());
    }

    @Test
    public void testCustomizeChannel() {
        registerAndRefresh(CustomizeChannelConfig.class);

        MessagingChannel<?> channel = get("testChannel", MessagingChannel.class);

        assertTrue(channel.cluster().topology().isEmpty());
    }
}
