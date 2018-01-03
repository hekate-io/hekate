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
import io.hekate.metrics.local.Probe;
import java.util.concurrent.atomic.AtomicLong;

class DefaultProbeMetric implements Metric {
    private final Probe probe;

    private final AtomicLong value = new AtomicLong();

    private final String name;

    private boolean failed;

    public DefaultProbeMetric(String name, Probe probe, long initVal) {
        assert name != null : "Name is null.";
        assert probe != null : "Probe is null.";

        this.probe = probe;
        this.name = name;

        value.set(initVal);
    }

    public boolean isFailed() {
        return failed;
    }

    public void setFailed(boolean failed) {
        this.failed = failed;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long value() {
        return value.get();
    }

    public long update() {
        long newValue = probe.getCurrentValue();

        value.set(newValue);

        return newValue;
    }

    @Override
    public String toString() {
        return Metric.class.getSimpleName() + "[name=" + name + ", value=" + value + ']';
    }
}
