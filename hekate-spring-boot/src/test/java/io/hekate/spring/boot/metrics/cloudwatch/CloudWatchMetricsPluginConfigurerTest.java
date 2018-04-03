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

package io.hekate.spring.boot.metrics.cloudwatch;

import com.amazonaws.http.IdleConnectionReaper;
import io.hekate.HekateTestProps;
import io.hekate.metrics.MetricFilterGroup;
import io.hekate.metrics.MetricNameFilter;
import io.hekate.metrics.MetricRegexFilter;
import io.hekate.metrics.cloudwatch.CloudWatchMetaDataProvider;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsConfig;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsPlugin;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class CloudWatchMetricsPluginConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class CloudWatchTestConfig extends HekateTestConfigBase {
        @Bean
        public CloudWatchMetaDataProvider awsMetaDataProvider() {
            return new CloudWatchMetaDataProvider() {
                @Override
                public String getInstanceId() {
                    return "TestInstanceId";
                }

                @Override
                public String getInstanceRegion() {
                    return "TestInstanceRegion";
                }
            };
        }
    }

    @BeforeClass
    public static void mayBeDisableTest() {
        Assume.assumeTrue(Boolean.valueOf(HekateTestProps.get("AWS_TEST_ENABLED")));
    }

    @Override
    public void tearDown() throws Exception {
        IdleConnectionReaper.shutdown();

        super.tearDown();
    }

    @Test
    public void test() {
        String awsAccessKey = HekateTestProps.get("AWS_TEST_ACCESS_KEY");
        String awsSecretKey = HekateTestProps.get("AWS_TEST_SECRET_KEY");
        String awsRegion = HekateTestProps.get("AWS_TEST_REGION");

        registerAndRefresh(new String[]{
            "hekate.metrics.cloudwatch.enable=true",
            "hekate.metrics.cloudwatch.regex-filters[0]=jvm\\..*",
            "hekate.metrics.cloudwatch.regex-filters[1]=hekate\\..*",
            "hekate.metrics.cloudwatch.name-filters[0]=jvm.free.mem",
            "hekate.metrics.cloudwatch.name-filters[1]=hekate.network.bytes.out",
            "hekate.metrics.cloudwatch.region=" + awsRegion,
            "hekate.metrics.cloudwatch.access-key=" + awsAccessKey,
            "hekate.metrics.cloudwatch.secret-key=" + awsSecretKey,
            "hekate.metrics.cloudwatch.publish-interval=" + Integer.MAX_VALUE,
        }, CloudWatchTestConfig.class);

        assertNotNull(get(LocalMetricsService.class));
        assertNotNull(get(CloudWatchMetricsPlugin.class));

        CloudWatchMetricsConfig cfg = get(CloudWatchMetricsConfig.class);

        assertEquals(awsRegion, cfg.getRegion());
        assertEquals(awsAccessKey, cfg.getAccessKey());
        assertEquals(awsSecretKey, cfg.getSecretKey());
        assertEquals(Integer.MAX_VALUE, cfg.getPublishInterval());

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
