/*
 * Copyright 2019 The Hekate Project
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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.LongAdder;

class LockRegionMetrics {
    private final LongAdder locksActive = new LongAdder();

    private final Counter locks;

    public LockRegionMetrics(String name, MeterRegistry metrics) {
        assert name != null : "Name is null.";
        assert metrics != null : "Meter registry is null.";

        locks = Counter.builder("hekate.lock.count")
            .tag("region", name)
            .register(metrics);

        Gauge.builder("hekate.lock.active", locksActive, LongAdder::doubleValue)
            .tag("region", name)
            .register(metrics);
    }

    public void onLock() {
        locks.increment();

        locksActive.increment();
    }

    public void onUnlock() {
        locksActive.decrement();
    }
}
