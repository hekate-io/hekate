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

package io.hekate.metrics;

/**
 * Probe for metrics that obtain their values from some third-party source.
 *
 * <p>
 * This interface represents a probe that can be registered within the {@link MetricsService} in order to provide a metric from some
 * third-party source. Probes can be registered either {@link MetricsServiceFactory#withMetric(MetricConfigBase)} statically} or
 * {@link MetricsService#register(ProbeConfig) dynamically}.
 * </p>
 *
 * <p>
 * Once probe is registered, the {@link MetricsService} will start polling it every {@link
 * MetricsServiceFactory#getRefreshInterval()} in order to get the latest probe value. Such value is cached within the service for the
 * duration of refresh interval and can be obtained via {@link MetricsService#metric(String)} method.
 * </p>
 *
 * <p>
 * For more details about metric types and their usage please see the documentation of {@link MetricsService} interface.
 * </p>
 *
 * @see MetricsService
 * @see ProbeConfig
 */
@FunctionalInterface
public interface Probe {
    /**
     * Returns the value of this probe.
     *
     * @return Probe value.
     */
    long getCurrentValue();
}
