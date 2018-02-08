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
 * Configuration for {@link CounterMetric}.
 *
 * <p>
 * Fore more details about counters and other metrics please see the documentation of {@link LocalMetricsService}.
 * </p>
 *
 * @see LocalMetricsServiceFactory#withMetric(MetricConfigBase)
 * @see LocalMetricsService#register(CounterConfig)
 */
public class CounterConfig extends MetricConfigBase<CounterConfig> {
    private boolean autoReset;

    private String totalName;

    /**
     * Constructs new instance.
     */
    public CounterConfig() {
        // No-op.
    }

    /**
     * Constructs new instance with the specified name.
     *
     * @param name Name of this counter (see {@link #setName(String)}).
     */
    public CounterConfig(String name) {
        setName(name);
    }

    /**
     * Returns {@code true} if this counter should be reset to its initial after every
     * {@link LocalMetricsServiceFactory#getRefreshInterval() refresh interval} (see {@link #setAutoReset(boolean)}).
     *
     * @return {@code true} if this counter should be automatically reset to its initial value.
     */
    public boolean isAutoReset() {
        return autoReset;
    }

    /**
     * Sets the flag that controls whether this counter should be automatically reset to 0 or should keep its value across {@link
     * LocalMetricsServiceFactory#getRefreshInterval() refresh intervals}.
     *
     * <p>
     * Default value of this parameter is {@code false}.
     * </p>
     *
     * <p>
     * <b>Note:</b> even if this flag is set to {@code true} it is still possible to track total value of a counter by specifying {@link
     * #setTotalName(String)}.
     * </p>
     *
     * @param autoReset {@code true} if this counter should be automatically reset to 0.
     */
    public void setAutoReset(boolean autoReset) {
        this.autoReset = autoReset;
    }

    /**
     * Fluent-style version of {@link #setAutoReset(boolean)}.
     *
     * @param autoReset {@code true} if this counter should be automatically reset to its initial value.
     *
     * @return This instance.
     */
    public CounterConfig withAutoReset(boolean autoReset) {
        setAutoReset(autoReset);

        return this;
    }

    /**
     * Returns the name of a metric that will hold the total value of this counter (see {@link #setTotalName(String)}).
     *
     * @return Name of a metric that will hold the total value of this counter.
     */
    public String getTotalName() {
        return totalName;
    }

    /**
     * Sets the name of a metric that will hold the total value of this counter. Can contain only alpha-numeric characters and
     * non-repeatable dots/hyphens.
     *
     * <p>
     * This parameter is optional. If specified then an additional metric of that name will be registered within the {@link
     * LocalMetricsService} and will hold the total value aggregated during the whole history of this counter.
     * </p>
     *
     * <p>
     * <b>Note:</b> In most cases this parameter makes sense only if {@link #setAutoReset(boolean)} is set to {@code true}.
     * </p>
     *
     * @param totalName Name of a metric that will hols the total value of this counter (can contain only alpha-numeric characters and
     * non-repeatable dots/hyphens).
     */
    public void setTotalName(String totalName) {
        this.totalName = totalName;
    }

    /**
     * Fluent-style version of {@link #setTotalName(String)}.
     *
     * @param totalName Name of a metric that will hols the total value of this counter.
     *
     * @return This instance.
     */
    public CounterConfig withTotalName(String totalName) {
        setTotalName(totalName);

        return this;
    }
}
