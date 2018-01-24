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

package io.hekate.metrics.local;

import io.hekate.core.jmx.JmxTypeName;
import java.util.Map;
import javax.management.MXBean;

/**
 * JMX interface for {@link LocalMetricsService}.
 */
@MXBean
@JmxTypeName("LocalMetricsService")
public interface LocalMetricsServiceJmx {
    /**
     * Returns the snapshot of all metrics.
     *
     * @return Snapshot of all metrics.
     *
     * @see LocalMetricsService#snapshot()
     */
    Map<String, Long> getMetricsSnapshot();

    /**
     * Returns the configuration value of {@link LocalMetricsServiceFactory#getRefreshInterval()}.
     *
     * @return Value of {@link LocalMetricsServiceFactory#getRefreshInterval()}.
     */
    long getRefreshInterval();
}
