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

package io.hekate.spring.boot.metrics.statsd;

import io.hekate.metrics.MetricFilterGroup;
import io.hekate.metrics.MetricNameFilter;
import io.hekate.metrics.MetricRegexFilter;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.statsd.StatsdMetricsConfig;
import io.hekate.metrics.statsd.StatsdMetricsPlugin;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class StatsdMetricsPluginConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class StatsdTestConfig extends HekateTestConfigBase {
        // No-op.
    }

    @Test
    public void test() {
        registerAndRefresh(new String[]{
            "hekate.metrics.statsd.enable=true",
            "hekate.metrics.statsd.regex-filters[0]=jvm\\..*",
            "hekate.metrics.statsd.regex-filters[1]=hekate\\..*",
            "hekate.metrics.statsd.name-filters[0]=jvm.free.mem",
            "hekate.metrics.statsd.name-filters[1]=hekate.network.bytes.out",
            "hekate.metrics.statsd.host=localhost",
            "hekate.metrics.statsd.port=8125",
            "hekate.metrics.statsd.batch-size=100500",
            "hekate.metrics.statsd.max-queue-size=100501",
        }, StatsdTestConfig.class);

        assertNotNull(get(LocalMetricsService.class));
        assertNotNull(get(StatsdMetricsPlugin.class));

        StatsdMetricsConfig cfg = get(StatsdMetricsConfig.class);

        assertEquals("localhost", cfg.getHost());
        assertEquals(8125, cfg.getPort());
        assertEquals(100500, cfg.getBatchSize());
        assertEquals(100501, cfg.getMaxQueueSize());

        MetricFilterGroup filterGroup = (MetricFilterGroup)cfg.getFilter();

        assertNotNull(filterGroup);
        assertEquals(filterGroup.toString(), 4, filterGroup.getFilters().size());

        MetricRegexFilter regexFilter1 = (MetricRegexFilter)filterGroup.getFilters().get(0);
        MetricRegexFilter regexFilter2 = (MetricRegexFilter)filterGroup.getFilters().get(1);
        MetricNameFilter nameFilter1 = (MetricNameFilter)filterGroup.getFilters().get(2);
        MetricNameFilter nameFilter2 = (MetricNameFilter)filterGroup.getFilters().get(3);

        assertEquals("jvm\\..*", regexFilter1.pattern());
        assertEquals("hekate\\..*", regexFilter2.pattern());
        assertEquals("jvm.free.mem", nameFilter1.metricName());
        assertEquals("hekate.network.bytes.out", nameFilter2.metricName());
    }
}
