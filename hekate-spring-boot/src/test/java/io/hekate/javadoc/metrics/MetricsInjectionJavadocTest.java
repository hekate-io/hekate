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

package io.hekate.javadoc.metrics;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.Metric;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.metrics.NamedCounter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import org.junit.Test;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import static org.junit.Assert.assertNotNull;

public class MetricsInjectionJavadocTest extends HekateAutoConfigurerTestBase {
    // Start:counter_bean
    @Component
    public static class MyBean {
        @NamedCounter("my-counter")
        private CounterMetric counter;

        // ... other fields and methods...
    }
    // End:counter_bean

    // Start:metric_bean
    @Component
    public static class SomeBean {
        @NamedCounter("my-counter")
        private Metric metric;

        // ... other fields and methods...
    }
    // End:metric_bean

    // Start:app
    @EnableHekate
    @SpringBootApplication
    public static class MyApp {
        @Bean
        public CounterConfig counterMetricConfig() {
            return new CounterConfig()
                .withName("my-counter")
                .withTotalName("my-counter.total")
                .withAutoReset(true);
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

        assertNotNull(get(MyBean.class).counter);
        assertNotNull(get(SomeBean.class).metric);
    }
}
