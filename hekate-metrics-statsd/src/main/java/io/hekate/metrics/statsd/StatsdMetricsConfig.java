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

package io.hekate.metrics.statsd;

import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.util.format.ToString;

/**
 * Configuration for {@link StatsdMetricsPlugin}.
 */
public class StatsdMetricsConfig {
    /** Default value (={@value}) for {@link #setMaxQueueSize(int)}. */
    public static final int DEFAULT_QUEUE_SIZE = 100;

    /** Default value (={@value}) for {@link #setPort(int)}. */
    public static final int DEFAULT_PORT = 8125;

    /** Default value (={@value}) for {@link #setBatchSize(int)}. */
    public static final int DEFAULT_BATCH_SIZE = 10;

    private String host;

    private int port = DEFAULT_PORT;

    private int batchSize = DEFAULT_BATCH_SIZE;

    private int maxQueueSize = DEFAULT_QUEUE_SIZE;

    private MetricFilter filter;

    /**
     * Returns the host address of a StatsD daemon (see {@link #setHost(String)}).
     *
     * @return Host address of a StatsD daemon.
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host address of a StatsD daemon.
     *
     * @param host Host address of a StatsD daemon.
     *
     * @see #setPort(int)
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Fluent-style version of {@link #setHost(String)}.
     *
     * @param host Host address of a StatsD daemon.
     *
     * @return This instance.
     */
    public StatsdMetricsConfig withHost(String host) {
        setHost(host);

        return this;
    }

    /**
     * Returns the network port of a StatsD daemon (see {@link #setPort(int)}).
     *
     * @return Network port of a StatsD daemon.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the network port of a StatsD daemon.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_PORT}.
     * </p>
     *
     * @param port Network port of a StatsD daemon.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Fluent-style version of {@link #setPort(int)}.
     *
     * @param port Network port of a StatsD daemon.
     *
     * @return This instance.
     */
    public StatsdMetricsConfig withPort(int port) {
        setPort(port);

        return this;
    }

    /**
     * Returns the maximum number of metrics that should be placed into a single UDP packet (see {@link #setBatchSize(int)}).
     *
     * @return Maximum number of metrics that should be placed into a single UDP packet.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Sets the maximum number of metrics that should be placed into a single UDP packet.
     *
     * <p>
     * If number of metrics that should be published exceeds the value of this parameter then metrics will be split into multiple UDP
     * packets and will be sent independently.
     * </p>
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_BATCH_SIZE}.
     * </p>
     *
     * @param batchSize Maximum number of metrics that should be placed into a single UDP packet.
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Fluent-style version of {@link #setBatchSize(int)}.
     *
     * @param batchSize Maximum number of metrics that should be placed into a single UDP packet.
     *
     * @return This instance.
     */
    public StatsdMetricsConfig withBatchSize(int batchSize) {
        setBatchSize(batchSize);

        return this;
    }

    /**
     * Returns the maximum size of a queue to buffer metrics for asynchronous publishing (see {@link #setMaxQueueSize(int)}).
     *
     * @return Maximum queue size.
     */
    public int getMaxQueueSize() {
        return maxQueueSize;
    }

    /**
     * Sets the the maximum size of a queue to buffer metrics for asynchronous publishing.
     *
     * <p>
     * On every {@link LocalMetricsServiceFactory#setRefreshInterval(long)} tick a new entry with the snapshot of current metrics is added
     * to the queue of this size. If there is no more space in the queue (f.e. due to some networking issues) then such entries will be
     * dropped and will not be published to StatsD.
     * </p>
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_QUEUE_SIZE}.
     * </p>
     *
     * @param maxQueueSize Maximum queue size.
     */
    public void setMaxQueueSize(int maxQueueSize) {
        this.maxQueueSize = maxQueueSize;
    }

    /**
     * Fluent style version of {@link #setMaxQueueSize(int)}.
     *
     * @param maxQueueSize Maximum queue size.
     *
     * @return This instance.
     */
    public StatsdMetricsConfig withMaxQueueSize(int maxQueueSize) {
        setMaxQueueSize(maxQueueSize);

        return this;
    }

    /**
     * Returns the metrics publishing filter (see {@link #setFilter(MetricFilter)}).
     *
     * @return Filter.
     */
    public MetricFilter getFilter() {
        return filter;
    }

    /**
     * Sets the metrics publishing filter.
     *
     * <p>
     * Only those metrics that get accepted by this filter will be published to StatsD.
     * </p>
     *
     * @param filter Filter.
     */
    public void setFilter(MetricFilter filter) {
        this.filter = filter;
    }

    /**
     * Fluent style version of {@link #setFilter(MetricFilter)}.
     *
     * @param filter Filter.
     *
     * @return This instance.
     */
    public StatsdMetricsConfig withFilter(MetricFilter filter) {
        setFilter(filter);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
