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

package io.hekate.javadoc.messaging;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.messaging.InjectChannel;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertNotNull;

public class MessagingInjectionJavadocTest extends HekateAutoConfigurerTestBase {
    // Start:channel_bean
    @Component
    public static class MyBean {
        @InjectChannel("my-channel")
        private MessagingChannel<Object> channel;

        // ... other fields and methods...
    }
    // End:channel_bean

    // Start:app
    @EnableHekate
    @SpringBootApplication
    public static class MyApp {
        @Bean
        public MessagingChannelConfig<Object> messagingChannelConfig() {
            return MessagingChannelConfig.unchecked()
                .withName("my-channel")
                .withReceiver(msg ->
                    System.out.println("Got message " + msg)
                );
        }

        // ... other beans and methods...
    }
    // End:app

    public static class FasterMyApp extends MyApp {
        @Bean
        public SeedNodeProvider seedNodeProvider() throws UnknownHostException {
            return new StaticSeedNodeProvider(new StaticSeedNodeProviderConfig()
                .withAddress(InetAddress.getLocalHost().getHostAddress() + ':' + NetworkServiceFactory.DEFAULT_PORT)
            );
        }
    }

    @Test
    public void testChannel() {
        registerAndRefresh(FasterMyApp.class);

        assertNotNull(get(MyBean.class).channel);
    }
}
