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
import io.hekate.metrics.local.CounterMetric;
import java.util.concurrent.atomic.LongAdder;

class DefaultCounterMetric implements CounterMetric {
    private final String name;

    private final String totalName;

    private final LongAdder counter;

    private final LongAdder total;

    private final boolean autoReset;

    public DefaultCounterMetric(String name, boolean autoReset, String totalName) {
        assert name != null : "Name is null.";
        assert !name.trim().isEmpty() : "Name is empty.";

        this.counter = new LongAdder();
        this.name = name;
        this.totalName = totalName;
        this.autoReset = autoReset;

        if (totalName == null) {
            this.total = null;
        } else {
            this.total = new LongAdder();
        }
    }

    @Override
    public void increment() {
        counter.add(1);
    }

    @Override
    public void decrement() {
        counter.add(-1);
    }

    @Override
    public void add(long value) {
        counter.add(value);
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

    public long totalValue() {
        if (total == null) {
            return counter.sum();
        } else {
            long totalVal = total.sum();
            long curVal = counter.sum();

            return totalVal + curVal;
        }
    }

    public long reset() {
        long val = counter.sumThenReset();

        if (total != null) {
            total.add(val);
        }

        return val;
    }

    public boolean isAutoReset() {
        return autoReset;
    }

    public boolean hasTotal() {
        return total != null;
    }

    public String totalName() {
        return totalName;
    }

    @Override
    public String toString() {
        return Metric.class.getSimpleName() + "[name=" + name + ", value=" + counter + ']';
    }
}
