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

import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of {@link MetricFilter} interface that allows grouping of multiple filters.
 */
public class MetricFilterGroup implements MetricFilter {
    private List<MetricFilter> filters;

    /**
     * Returns {@code true} if at least one of the registered filters accepts the specified metric.
     *
     * @param metric Metric.
     *
     * @return {@code true} if at least one of the registered filters accepts the specified metric.
     */
    @Override
    public boolean accept(Metric metric) {
        for (MetricFilter filter : filters) {
            if (filter != null && filter.accept(metric)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns filters of this group.
     *
     * @return Filters.
     */
    public List<MetricFilter> getFilters() {
        return filters;
    }

    /**
     * Sets filters of this group.
     *
     * @param filters Filters.
     */
    public void setFilters(List<MetricFilter> filters) {
        this.filters = filters;
    }

    /**
     * Fluent-style version of {@link #setFilters(List)}.
     *
     * @param filter Filter to be added to this group.
     *
     * @return This instance.
     */
    public MetricFilterGroup withFilter(MetricFilter filter) {
        if (filters == null) {
            filters = new ArrayList<>();
        }

        filters.add(filter);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
