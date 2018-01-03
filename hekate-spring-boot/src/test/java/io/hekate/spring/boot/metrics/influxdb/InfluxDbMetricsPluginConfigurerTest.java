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

package io.hekate.spring.boot.metrics.influxdb;

import io.hekate.HekateTestProps;
import io.hekate.metrics.influxdb.InfluxDbMetricsConfig;
import io.hekate.metrics.influxdb.InfluxDbMetricsPlugin;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class InfluxDbMetricsPluginConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class InfluxDbTestConfig extends HekateTestConfigBase {
        // No-op.
    }

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(Boolean.valueOf(HekateTestProps.get("INFLUXDB_ENABLED")));
    }

    @Test
    public void test() {
        String url = HekateTestProps.get("INFLUXDB_URL");
        String user = HekateTestProps.get("INFLUXDB_USER");
        String password = HekateTestProps.get("INFLUXDB_PASSWORD");
        String database = "testdb";

        registerAndRefresh(new String[]{
            "hekate.metrics.influxdb.enable=true",
            "hekate.metrics.influxdb.url=" + url,
            "hekate.metrics.influxdb.user=" + user,
            "hekate.metrics.influxdb.password=" + password,
            "hekate.metrics.influxdb.database=" + database,
            "hekate.metrics.influxdb.max-queue-size=100500",
            "hekate.metrics.influxdb.timeout=100501"
        }, InfluxDbTestConfig.class);

        assertNotNull(get(LocalMetricsService.class));
        assertNotNull(get(InfluxDbMetricsPlugin.class));

        InfluxDbMetricsConfig cfg = get(InfluxDbMetricsConfig.class);

        assertEquals(url, cfg.getUrl());
        assertEquals(user, cfg.getUser());
        assertEquals(password, cfg.getPassword());
        assertEquals(100500, cfg.getMaxQueueSize());
        assertEquals(100501, cfg.getTimeout());
    }
}
