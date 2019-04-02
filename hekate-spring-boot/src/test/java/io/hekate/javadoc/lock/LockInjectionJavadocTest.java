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

package io.hekate.javadoc.lock;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.lock.InjectLock;
import io.hekate.spring.boot.lock.InjectLockRegion;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertNotNull;

public class LockInjectionJavadocTest extends HekateAutoConfigurerTestBase {
    // Start:region_bean
    @Component
    public static class MyBean {
        @InjectLockRegion("my-region")
        private LockRegion region;

        // ... other fields and methods...
    }
    // End:region_bean

    // Start:region_app
    @EnableHekate
    @SpringBootApplication
    public static class MyApp {
        @Bean
        public LockRegionConfig lockRegionConfig() {
            return new LockRegionConfig().withName("my-region");
        }

        // ... other beans and methods...
    }
    // End:region_app

    // Start:lock_bean
    @Component
    public static class ExampleBean {
        @InjectLock(name = "my-lock", region = "my-region")
        private DistributedLock lock;

        // ... other fields and methods...
    }
    // End:lock_bean

    // Start:lock_app
    @EnableHekate
    @SpringBootApplication
    public static class ExampleApp {
        @Bean
        public LockRegionConfig lockRegionConfig() {
            return new LockRegionConfig().withName("my-region");
        }

        // ... other beans and methods...
    }
    // End:lock_app

    public static class FasterMyApp extends MyApp {
        @Bean
        public SeedNodeProvider seedNodeProvider() throws UnknownHostException {
            return new StaticSeedNodeProvider(new StaticSeedNodeProviderConfig()
                .withAddress(InetAddress.getLocalHost().getHostAddress() + ':' + NetworkServiceFactory.DEFAULT_PORT)
            );
        }
    }

    public static class FasterExampleApp extends ExampleApp {
        @Bean
        public SeedNodeProvider seedNodeProvider() throws UnknownHostException {
            return new StaticSeedNodeProvider(new StaticSeedNodeProviderConfig()
                .withAddress(InetAddress.getLocalHost().getHostAddress() + ':' + NetworkServiceFactory.DEFAULT_PORT)
            );
        }
    }

    @Test
    public void regionExample() {
        registerAndRefresh(FasterMyApp.class);

        assertNotNull(get(MyBean.class).region);
    }

    @Test
    public void lockExample() {
        registerAndRefresh(FasterExampleApp.class);

        assertNotNull(get(ExampleBean.class).lock);
    }
}
