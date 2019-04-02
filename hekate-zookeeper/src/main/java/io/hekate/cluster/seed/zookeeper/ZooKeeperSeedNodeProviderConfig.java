/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.cluster.seed.zookeeper;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;

/**
 * Configuration for {@link ZooKeeperSeedNodeProvider}.
 *
 * @see ZooKeeperSeedNodeProvider#ZooKeeperSeedNodeProvider(ZooKeeperSeedNodeProviderConfig)
 */
public class ZooKeeperSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setConnectTimeout(int)}. */
    public static final int DEFAULT_CONNECT_TIMEOUT = 5 * 1000;

    /** Default value (={@value}) for {@link #setSessionTimeout(int)}. */
    public static final int DEFAULT_SESSION_TIMEOUT = 10 * 1000;

    /** Default value (={@value}) for {@link #setCleanupInterval(int)}. */
    public static final int DEFAULT_CLEANUP_INTERVAL = 60 * 1000;

    /** Default value (={@value}) for {@link #setBasePath(String)}. */
    public static final String DEFAULT_BASE_PATH = "/hekate/cluster";

    /** See {@link #setConnectionString(String)}. */
    private String connectionString;

    /** See {@link #setConnectTimeout(int)}. */
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;

    /** See {@link #setSessionTimeout(int)}. */
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    /** See {@link #setBasePath(String)}. */
    private String basePath = DEFAULT_BASE_PATH;

    /** See {@link #setCleanupInterval(int)}. */
    private int cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    /**
     * Returns the list of ZooKeeper servers to connect to (see {@link #setConnectionString(String)}).
     *
     * @return ZooKeeper connection string.
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Set the list of ZooKeeper servers to connect to.
     *
     * <p>
     * Connection string can be specified as a comma-separated list of host:port addresses (f.e. {@code 127.0.1:2181,127.0.01:2182})
     * </p>
     *
     * @param connectionString Comma-separated list of addresses.
     */
    public void setConnectionString(String connectionString) {
        this.connectionString = connectionString;
    }

    /**
     * Fluent-style version of {@link #setConnectionString(String)}.
     *
     * @param connectionString Comma-separated list of addresses.
     *
     * @return This instance.
     */
    public ZooKeeperSeedNodeProviderConfig withConnectionString(String connectionString) {
        setConnectionString(connectionString);

        return this;
    }

    /**
     * Returns the ZooKeeper connection timeout in milliseconds (see {@link #setConnectTimeout(int)}).
     *
     * @return Timeout in milliseconds.
     */
    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * Sets the ZooKeeper connection timeout in milliseconds.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_CONNECT_TIMEOUT}.
     * </p>
     *
     * @param connectTimeout Timeout in milliseconds.
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    /**
     * Fluent-style version of {@link #setConnectTimeout(int)}.
     *
     * @param connectTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public ZooKeeperSeedNodeProviderConfig withConnectTimeout(int connectTimeout) {
        setConnectTimeout(connectTimeout);

        return this;
    }

    /**
     * Returns the ZooKeeper session timeout in milliseconds (see {@link #setSessionTimeout(int)}).
     *
     * @return Timeout in milliseconds.
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the ZooKeeper session timeout in milliseconds.
     *
     * <p>
     * Note that {@link ZooKeeperSeedNodeProvider} uses short-lived connection when interacting with ZooKeeper server (i.e. it creates a
     * new connection for each operation and then closes it once operation is completed). Thus, it is not necessary to set this parameter to
     * a large value.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_SESSION_TIMEOUT}.
     * </p>
     *
     * @param sessionTimeout Timeout in milliseconds.
     */
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Fluent-style version of {@link #setSessionTimeout(int)}.
     *
     * @param sessionTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public ZooKeeperSeedNodeProviderConfig withSessionTimeout(int sessionTimeout) {
        setSessionTimeout(sessionTimeout);

        return this;
    }

    /**
     * Returns the base path to store seed nodes information in ZooKeeper (see {@link #setBasePath(String)}).
     *
     * @return Base path to store seed nodes.
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Sets the base path to store seed nodes information in ZooKeeper.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_BASE_PATH}.
     * </p>
     *
     * @param basePath Base path to store seed nodes.
     */
    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Fluent-style version of {@link #setBasePath(String)}.
     *
     * @param basePath Base path to store seed nodes.
     *
     * @return This instance.
     */
    public ZooKeeperSeedNodeProviderConfig withBasePath(String basePath) {
        setBasePath(basePath);

        return this;
    }

    /**
     * Returns the time interval in milliseconds between stale node cleanup runs (see {@link #setCleanupInterval(int)}).
     *
     * @return Time interval in milliseconds.
     */
    public int getCleanupInterval() {
        return cleanupInterval;
    }

    /**
     * Sets the time interval in milliseconds between stale node cleanup runs.
     *
     * <p>Default value of this parameter is {@value #DEFAULT_CLEANUP_INTERVAL}.</p>
     *
     * <p>
     * For more details please see the documentation of {@link SeedNodeProvider}.
     * </p>
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @see SeedNodeProvider#cleanupInterval()
     */
    public void setCleanupInterval(int cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Fluent-style version of {@link #setCleanupInterval(int)}.
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public ZooKeeperSeedNodeProviderConfig withCleanupInterval(int cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
