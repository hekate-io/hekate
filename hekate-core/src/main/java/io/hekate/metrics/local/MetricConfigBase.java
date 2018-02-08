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

import io.hekate.util.format.ToString;

/**
 * Abstract base class for metric configurations.
 *
 * @param <T> Concrete configuration type.
 */
public abstract class MetricConfigBase<T extends MetricConfigBase<T>> {
    private String name;

    /**
     * Returns the name of this metric (see {@link #setName(String)}).
     *
     * @return Name of this metric.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the name of this metric. Can contain only alpha-numeric characters and non-repeatable dots/hyphens.
     *
     * @param name Name of this metric (can contain only alpha-numeric characters and non-repeatable dots/hyphens).
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Fluent-style version of {@link #setName(String)}.
     *
     * @param name Name of this metric.
     *
     * @return This instance.
     */
    @SuppressWarnings("unchecked")
    public T withName(String name) {
        setName(name);

        return (T)this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
