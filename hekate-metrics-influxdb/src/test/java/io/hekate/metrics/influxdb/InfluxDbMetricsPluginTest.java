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

package io.hekate.metrics.influxdb;

import io.hekate.core.HekateTestInstance;
import io.hekate.metrics.MetricsServiceFactory;
import io.hekate.metrics.ProbeConfig;
import java.util.concurrent.CountDownLatch;
import org.junit.Test;

public class InfluxDbMetricsPluginTest extends InfluxDbMetricsTestBase {

    @Test
    public void test() throws Exception {
        CountDownLatch published = new CountDownLatch(1);

        HekateTestInstance node = createInstance(boot -> {
            boot.withPlugin(new InfluxDbMetricsPlugin(new InfluxDbMetricsConfig()
                .withUrl(url)
                .withDatabase(database)
                .withUser(user)
                .withPassword(password))
            );

            boot.findOrRegister(MetricsServiceFactory.class)
                .withRefreshInterval(100)
                .withMetric(new ProbeConfig("test_metric")
                    .withProbe(() -> 1000)
                )
                .withListener(event -> published.countDown());
        }).join();

        await(published);

        busyWait("test metric value", () -> {
            Long val = loadLatestValue("test_metric");

            return val != null && val == 1000;
        });

        node.leave();
    }
}
