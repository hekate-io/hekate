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

package io.hekate.spring.boot.metrics;

import io.hekate.core.Hekate;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricsService;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.endpoint.PublicMetrics;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for exporting metrics to Spring Boot Actuator.
 */
@Configuration
@ConditionalOnHekateEnabled
@ConditionalOnClass(PublicMetrics.class)
@AutoConfigureBefore(EndpointAutoConfiguration.class)
@AutoConfigureAfter(HekateConfigurer.class)
public class HekateMetricsPublisherConfigurer {
    /**
     * Exposes all metrics from a {@link MetricsService} obtained form the specified {@link Hekate} instance.
     *
     * @param node Node.
     *
     * @return Metrics for Spring Boot Actuator.
     */
    @Bean
    public PublicMetrics publicMetrics(Hekate node) {
        return () -> {
            if (node.has(MetricsService.class)) {
                Map<String, Metric> localMetrics = node.get(MetricsService.class).allMetrics();

                List<org.springframework.boot.actuate.metrics.Metric<?>> publicMetrics = new ArrayList<>(localMetrics.size());

                localMetrics.forEach((name, metric) ->
                    publicMetrics.add(new org.springframework.boot.actuate.metrics.Metric<Number>(name, metric.getValue()))
                );

                return publicMetrics;
            } else {
                return Collections.emptyList();
            }
        };
    }
}
