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

/**
 * Configuration for {@link Probe}.
 *
 * <p>
 * Fore more details about metrics and probes please see the documentation of {@link LocalMetricsService}.
 * </p>
 *
 * @see LocalMetricsServiceFactory#withMetric(MetricConfigBase)
 * @see LocalMetricsService#register(ProbeConfig)
 */
public class ProbeConfig extends MetricConfigBase<ProbeConfig> {
    private long initValue;

    private Probe probe;

    /**
     * Constructs new instance.
     */
    public ProbeConfig() {
        // No-op.
    }

    /**
     * Constructs new instance with the specified metric name.
     *
     * @param name Name of this probe (see {@link #setName(String)}).
     */
    public ProbeConfig(String name) {
        setName(name);
    }

    /**
     * Returns the probe implementation.
     *
     * @return Probe.
     */
    public Probe getProbe() {
        return probe;
    }

    /**
     * Sets the implementation of this probe.
     *
     * @param probe Implementation of this probe.
     */
    public void setProbe(Probe probe) {
        this.probe = probe;
    }

    /**
     * Fluent-style version of {@link #setProbe(Probe)}.
     *
     * @param probe Implementation of this probe.
     *
     * @return This instance.
     */
    public ProbeConfig withProbe(Probe probe) {
        setProbe(probe);

        return this;
    }

    /**
     * Returns the initial value of this probe (see {@link #setInitValue(long)}).
     *
     * @return Initial value.
     */
    public long getInitValue() {
        return initValue;
    }

    /**
     * Sets the initial value of this probe.
     *
     * <p>
     * This value will be used by the {@link LocalMetricsService} unless it obtains the first real value from the {@link
     * #setProbe(Probe) probe implementation}.
     * </p>
     *
     * <p>
     * Default value of this parameter is 0.
     * </p>
     *
     * @param initValue Initial value.
     */
    public void setInitValue(long initValue) {
        this.initValue = initValue;
    }

    /**
     * Fluent-style version of {@link #setInitValue(long)}.
     *
     * @param initValue Initial value.
     *
     * @return This instance.
     */
    public ProbeConfig withInitValue(long initValue) {
        setInitValue(initValue);

        return this;
    }
}
