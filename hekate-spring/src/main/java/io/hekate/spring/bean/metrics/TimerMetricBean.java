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

import io.hekate.metrics.local.TimerConfig;
import io.hekate.metrics.local.TimerMetric;
import io.hekate.spring.bean.HekateBaseBean;
import org.springframework.beans.factory.annotation.Required;

/**
 * Imports {@link TimerMetric} into a Spring context.
 */
public class TimerMetricBean extends HekateBaseBean<TimerMetric> {
    private String name;

    @Override
    public TimerMetric getObject() throws Exception {
        return getSource().localMetrics().timer(name);
    }

    @Override
    public Class<TimerMetric> getObjectType() {
        return TimerMetric.class;
    }

    /**
     * Returns the timer name (see {@link #setName(String)}).
     *
     * @return Timer name.
     */
    public String getName() {
        return name;
    }

    /**
     * Sets the timer name.
     *
     * @param name Timer name.
     *
     * @see TimerConfig#setName(String)
     */
    @Required
    public void setName(String name) {
        this.name = name;
    }
}
