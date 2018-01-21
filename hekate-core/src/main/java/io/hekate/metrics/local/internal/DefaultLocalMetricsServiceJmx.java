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

package io.hekate.metrics.local.internal;

import io.hekate.metrics.Metric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceJmx;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

class DefaultLocalMetricsServiceJmx implements LocalMetricsServiceJmx {
    private final LocalMetricsService service;

    public DefaultLocalMetricsServiceJmx(LocalMetricsService service) {
        this.service = service;
    }

    @Override
    public Map<String, Long> getMetricsSnapshot() {
        return service.snapshot().allMetrics().values().stream().collect(toMap(Metric::name, Metric::value));
    }

    @Override
    public long getRefreshInterval() {
        return service.refreshInterval();
    }
}
