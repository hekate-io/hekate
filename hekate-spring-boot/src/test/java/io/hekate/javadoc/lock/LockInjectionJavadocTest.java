/*
 * Copyright 2020 The Hekate Project
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
import io.hekate.network.NetworkServiceFactory;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.lock.InjectLock;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertNotNull;

public class LockInjectionJavadocTest extends HekateAutoConfigurerTestBase {
    // Start:bean
    @Component
    public static class MyBean {
        @InjectLock(name = "my-lock", region = "my-region")
        private DistributedLock lock;

        // ... other fields and methods...
    }
    // End:bean

    @ComponentScan(excludeFilters = @ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = MyApp.class))
    public static class FasterMyApp extends MyApp {
        @Bean
        public SeedNodeProvider seedNodeProvider() throws UnknownHostException {
            return new StaticSeedNodeProvider(new StaticSeedNodeProviderConfig()
                .withAddress(InetAddress.getLocalHost().getHostAddress() + ':' + NetworkServiceFactory.DEFAULT_PORT)
            );
        }
    }

    @Test
    public void lockExample() {
        registerAndRefresh(FasterMyApp.class);

        assertNotNull(get(MyBean.class).lock);
    }
}
