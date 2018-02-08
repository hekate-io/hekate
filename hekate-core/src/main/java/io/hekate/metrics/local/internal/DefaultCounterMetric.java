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
import io.hekate.metrics.local.CounterMetric;
import java.util.concurrent.atomic.LongAdder;

class DefaultCounterMetric implements CounterMetric {
    private final String name;

    private final LongAdder counter;

    private final CounterMetric total;

    private final boolean autoReset;

    public DefaultCounterMetric(String name, boolean autoReset) {
        this(name, autoReset, null);
    }

    public DefaultCounterMetric(String name, boolean autoReset, CounterMetric total) {
        assert name != null : "Name is null.";
        assert !name.isEmpty() : "Name is empty.";

        this.counter = new LongAdder();
        this.name = name;
        this.total = total;
        this.autoReset = autoReset;
    }

    @Override
    public void increment() {
        counter.add(1);

        if (total != null) {
            total.increment();
        }
    }

    @Override
    public void decrement() {
        counter.add(-1);
    }

    @Override
    public void add(long value) {
        counter.add(value);

        if (total != null) {
            total.add(value);
        }
    }

    @Override
    public void subtract(long value) {
        counter.add(-value);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long value() {
        return counter.sum();
    }

    public long getAndReset() {
        return counter.sumThenReset();
    }

    @Override
    public boolean isAutoReset() {
        return autoReset;
    }

    @Override
    public boolean hasTotal() {
        return total != null;
    }

    @Override
    public Metric total() {
        return total;
    }

    @Override
    public String toString() {
        return Metric.class.getSimpleName() + "[name=" + name + ", value=" + counter + ']';
    }
}
