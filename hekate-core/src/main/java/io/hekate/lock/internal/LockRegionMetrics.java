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

package io.hekate.lock.internal;

import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;

class LockRegionMetrics {
    private final CounterMetric locks;

    private final CounterMetric locksActive;

    public LockRegionMetrics(String name, LocalMetricsService service) {
        assert name != null : "Name is null.";
        assert service != null : "Metrics service is null.";

        String prefix = name + ".locks";

        locksActive = service.register(new CounterConfig(prefix + ".active"));

        locks = service.register(new CounterConfig()
            .withName(prefix + ".interim")
            .withTotalName(prefix + ".total")
            .withAutoReset(true)
        );
    }

    public void onLock() {
        locksActive.increment();

        locks.increment();
    }

    public void onUnlock() {
        locksActive.decrement();
    }
}
