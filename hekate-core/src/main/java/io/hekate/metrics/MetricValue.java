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

package io.hekate.metrics;

/**
 * Immutable {@link Metric}.
 */
public class MetricValue implements Metric {
    private final String name;

    private final long value;

    /**
     * Constructs a new instance.
     *
     * @param name Metric name.
     * @param value Metric value.
     */
    public MetricValue(String name, long value) {
        this.name = name;
        this.value = value;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public long value() {
        return value;
    }

    @Override
    public String toString() {
        return Metric.class.getSimpleName() + "[name=" + name + ", value=" + value + ']';
    }
}
