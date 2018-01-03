/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.javadoc;

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.metrics.influxdb.InfluxDbMetricsConfig;
import io.hekate.metrics.influxdb.InfluxDbMetricsPlugin;
import io.hekate.metrics.influxdb.InfluxDbMetricsTestBase;
import org.junit.Test;

public class InfluxDbMetricsPluginJavadocTest extends InfluxDbMetricsTestBase {
    @Test
    public void test() throws Exception {
        //Start:configure
        InfluxDbMetricsConfig influxDbCfg = new InfluxDbMetricsConfig()
            .withUrl("http://my-influxdb-host:8086")
            .withDatabase("my_database")
            .withUser("my_user")
            .withPassword("my_password");
        //End:configure

        influxDbCfg.setUrl(url);
        influxDbCfg.setDatabase(database);
        influxDbCfg.setUser(user);
        influxDbCfg.setPassword(password);

        // Start:boot
        Hekate node = new HekateBootstrap()
            .withPlugin(new InfluxDbMetricsPlugin(influxDbCfg))
            .join();
        // End:boot

        try {
            busyWait("test metric value", () -> {
                Long val = loadLatestValue("hekate.cluster.gossip.update");

                return val != null;
            });
        } finally {
            node.leave();
        }
    }
}
