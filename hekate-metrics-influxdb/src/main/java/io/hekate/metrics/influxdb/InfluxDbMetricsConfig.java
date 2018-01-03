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

package io.hekate.metrics.influxdb;

import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

/**
 * Configuration for {@link InfluxDbMetricsPlugin}.
 */
public class InfluxDbMetricsConfig {
    /** Default value (={@value}) for {@link #setMaxQueueSize(int)}. */
    public static final int DEFAULT_QUEUE_SIZE = 100;

    /** Default value (={@value}) in milliseconds for {@link #setTimeout(long)}. */
    public static final int DEFAULT_TIMEOUT = 3000;

    private String url;

    private String database;

    private String user;

    @ToStringIgnore
    private String password;

    private int maxQueueSize = DEFAULT_QUEUE_SIZE;

    private long timeout = DEFAULT_TIMEOUT;

    private MetricFilter filter;

    /**
     * Returns the URL of InfluxDB database (see {@link #setUrl(String)}).
     *
     * @return URL of InfluxDB database.
     */
    public String getUrl() {
        return url;
    }

    /**
     * Sets the URL of InfluxDB database. For example 'http://my-influxdb-host:8086'.
     *
     * @param url URL of InfluxDB database.
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * Fluent style version of {@link #setUrl(String)}.
     *
     * @param url URL of InfluxDB database.
     *
     * @return This instance.
     */
    public InfluxDbMetricsConfig withUrl(String url) {
        setUrl(url);

        return this;
    }

    /**
     * Returns the name of InfluxDB database (see {@link #setDatabase(String)}).
     *
     * @return name of InfluxDB database.
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Sets the name of InfluxDB database.
     *
     * @param database Name of InfluxDB database.
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Fluent style version of {@link #setDatabase(String)}.
     *
     * @param database Name of InfluxDB database.
     *
     * @return This instance.
     */
    public InfluxDbMetricsConfig withDatabase(String database) {
        setDatabase(database);

        return this;
    }

    /**
     * Returns the username for accessing InfluxDB database (see {@link #setUser(String)}).
     *
     * @return Username.
     */
    public String getUser() {
        return user;
    }

    /**
     * Sets the username for accessing InfluxDB database.
     *
     * @param user Username.
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Fluent style version of {@link #setUser(String)}.
     *
     * @param user Username.
     *
     * @return This instance.
     */
    public InfluxDbMetricsConfig withUser(String user) {
        setUser(user);

        return this;
    }

    /**
     * Returns the password for accessing InfluxDB database (see {@link #setPassword(String)}).
     *
     * @return Password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password for accessing InfluxDB database.
     *
     * @param password Password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Fluent style version of {@link #setPassword(String)}.
     *
     * @param password Password.
     *
     * @return This instance.
     */
    public InfluxDbMetricsConfig withPassword(String password) {
        setPassword(password);

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
     * to the queue of this size. If there is no more space in the queue (f.e. due to slow network connection between the publisher and
     * InfluxDB) then such entries will be dropped and will not be published to InfluxDB.
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
    public InfluxDbMetricsConfig withMaxQueueSize(int maxQueueSize) {
        setMaxQueueSize(maxQueueSize);

        return this;
    }

    /**
     * Returns the timeout (in milliseconds) of networking operations while publishing metrics to InfluxDB (see {@link #setTimeout(long)}).
     *
     * @return Timeout in milliseconds.
     */
    public long getTimeout() {
        return timeout;
    }

    /**
     * Sets the timeout (in milliseconds) for networking operations while publishing metrics to InfluxDB.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_TIMEOUT}.
     * </p>
     *
     * @param timeout Timeout in milliseconds.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /**
     * Fluent style version of {@link #setTimeout(long)}.
     *
     * @param timeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public InfluxDbMetricsConfig withTimeout(long timeout) {
        setTimeout(timeout);

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
     * Only those metrics that get accepted by this filter will be published to InfluxDB.
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
    public InfluxDbMetricsConfig withFilter(MetricFilter filter) {
        setFilter(filter);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
