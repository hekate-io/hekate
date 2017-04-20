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

package io.hekate.metrics.local.internal;

import io.hekate.metrics.Metric;

class CounterTotalMetric implements Metric {
    private final DefaultCounterMetric counter;

    public CounterTotalMetric(DefaultCounterMetric counter) {
        assert counter != null : "Counter is null.";

        this.counter = counter;
    }

    @Override
    public String getName() {
        return counter.getTotalName();
    }

    @Override
    public long getValue() {
        return counter.getTotalValue();
    }

    @Override
    public String toString() {
        return Metric.class.getSimpleName() + "[name=" + getName() + ", value=" + getValue() + ']';
    }
}
