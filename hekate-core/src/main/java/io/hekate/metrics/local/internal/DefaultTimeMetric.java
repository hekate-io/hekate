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
import io.hekate.metrics.local.TimeSpan;
import io.hekate.metrics.local.TimerMetric;
import io.hekate.util.time.SystemTimeSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

class DefaultTimeMetric implements TimerMetric {
    static class Aggregate {
        private final long avgTime;

        private final long rate;

        public Aggregate(long avgTime, long rate) {
            this.avgTime = avgTime;
            this.rate = rate;
        }

        public long avgTime() {
            return avgTime;
        }

        public long rate() {
            return rate;
        }
    }

    private class Span implements TimeSpan {
        private final long startedAt = time.nanoTime();

        @Override
        public void close() {
            update(time.nanoTime() - startedAt);
        }
    }

    private final String name;

    private final TimeUnit timeUnit;

    private final SystemTimeSupplier time;

    private final DefaultCounterMetric count;

    private final LongAdder sum = new LongAdder();

    private final String rateName;

    public DefaultTimeMetric(String name, TimeUnit timeUnit, SystemTimeSupplier time, String rateName) {
        assert name != null : "Name is null.";
        assert timeUnit != null : "Time unit is null.";
        assert time != null : "Time is null.";

        this.name = name;
        this.timeUnit = timeUnit;
        this.time = time;
        this.rateName = rateName;
        this.count = new DefaultCounterMetric(rateName == null ? name + ".rate" : rateName, false);
    }

    @Override
    public boolean hasRate() {
        return rateName != null;
    }

    @Override
    public Metric rate() {
        return count;
    }

    @Override
    public TimeUnit timeUnit() {
        return timeUnit;
    }

    @Override
    public TimeSpan start() {
        return new Span();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long value() {
        long s = sum.sum();
        long c = count.value();

        return c > 0 ? s / c : 0;
    }

    public Aggregate aggregateAndReset() {
        long totalTime = sum.sumThenReset();
        long rate = count.getAndReset();

        long avgTime = rate > 0 ? totalTime / rate : 0;

        return new Aggregate(avgTime, rate);
    }

    private void update(long time) {
        count.add(1);

        sum.add(timeUnit.convert(time, TimeUnit.NANOSECONDS));
    }
}
