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

package io.hekate.spring.boot.metrics;

import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

import static org.junit.Assert.assertNotNull;

public class HekateClusterMetricsServiceConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableAutoConfiguration
    public static class ClusterMetricsTestConfig extends HekateTestConfigBase {
        @Autowired(required = false)
        private ClusterMetricsService clusterMetricsService;
    }

    @Test
    public void test() throws Exception {
        registerAndRefresh(ClusterMetricsTestConfig.class);

        assertNotNull(get("clusterMetricsService", ClusterMetricsService.class));
        assertNotNull(get(ClusterMetricsTestConfig.class).clusterMetricsService);

        assertNotNull(getNode().clusterMetrics());
    }
}
