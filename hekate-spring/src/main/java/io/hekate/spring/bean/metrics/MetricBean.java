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

package io.hekate.spring.bean.metrics;

import io.hekate.metrics.Metric;
import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Imports {@link Metric} into a Spring context.
 */
public class MetricBean extends HekateBaseBean<Metric> {
    private String name;

    @Override
    public Metric getObject() throws Exception {
        return getSource().localMetrics().metric(name);
    }

    @Override
    public Class<Metric> getObjectType() {
        return Metric.class;
    }

    /**
     * Returns the metric name (see {@link #setName(String)}).
     *
     * @return Metric name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the metric name.
     *
     * @param name Metric name.
     *
     * @see Metric#name()
     */
    @Required
    public void setName(String name) {
        this.name = name;
    }
}
