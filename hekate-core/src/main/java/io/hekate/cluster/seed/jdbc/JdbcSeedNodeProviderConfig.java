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

package io.hekate.cluster.seed.jdbc;

import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.util.format.ToString;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * Configuration for {@link JdbcSeedNodeProvider}.
 *
 * <h2>Database structure</h2>
 * <p>
 * By default the following database table should exist in the target database:
 * </p>
 * <pre>{@code
 * CREATE TABLE cluster_nodes (
 *      cluster_name VARCHAR,
 *      host VARCHAR,
 *      port NUMBER
 * )
 * }</pre>
 *
 * <p>
 * It is possible to modify table and column names by using the following properties:
 * </p>
 * <ul>
 * <li>{@link #setTable(String)} - table name (default is {@value #DEFAULT_TABLE})</li>
 * <li>{@link #setClusterColumn(String)} - name of a column to store cluster name (default is {@value #DEFAULT_CLUSTER_COLUMN})</li>
 * <li>{@link #setHostColumn(String)} - name of a column to store host address (default is {@value #DEFAULT_HOST_COLUMN})</li>
 * <li>{@link #setPortColumn(String)} - name of a column to store port value (default is {@value #DEFAULT_PORT_COLUMN})</li>
 * </ul>
 *
 * <h2>Database connectivity</h2>
 * <p>
 * Database connectivity is controlled by the following properties:
 * </p>
 * <ul>
 * <li>{@link #setDataSource(DataSource)} - JDBC datasource to be used for obtaining database connections</li>
 * <li>{@link #setQueryTimeout(int)} - optional timeout value for SQL queries (see {@link Statement#setQueryTimeout(int)})</li>
 * </ul>
 *
 * @see JdbcSeedNodeProvider
 */
public class JdbcSeedNodeProviderConfig {
    /** Default value (={@value}) for {@link #setCleanupInterval(long)}. */
    public static final long DEFAULT_CLEANUP_INTERVAL = 60 * 1000;

    /** Default value (={@value}) for {@link #setTable(String)}. */
    public static final String DEFAULT_TABLE = "cluster_nodes";

    /** Default value (={@value}) for {@link #setHostColumn(String)}. */
    public static final String DEFAULT_HOST_COLUMN = "host";

    /** Default value (={@value}) for {@link #setPortColumn(String)}. */
    public static final String DEFAULT_PORT_COLUMN = "port";

    /** Default value (={@value}) for {@link #setClusterColumn(String)}. */
    public static final String DEFAULT_CLUSTER_COLUMN = "cluster_name";

    private DataSource dataSource;

    private long cleanupInterval = DEFAULT_CLEANUP_INTERVAL;

    private int queryTimeout;

    private String table = DEFAULT_TABLE;

    private String hostColumn = DEFAULT_HOST_COLUMN;

    private String portColumn = DEFAULT_PORT_COLUMN;

    private String clusterColumn = DEFAULT_CLUSTER_COLUMN;

    /**
     * Returns the data source (see {@link #setDataSource(DataSource)}).
     *
     * @return Data source
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Sets the data source.
     *
     * @param dataSource Data source.
     */
    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Fluent-style version of {@link #setDataSource(DataSource)}.
     *
     * @param dataSource Data source.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withDataSource(DataSource dataSource) {
        setDataSource(dataSource);

        return this;
    }

    /**
     * Returns the table name (see {@link #setTable(String)}).
     *
     * @return Table name.
     */
    public String getTable() {
        return table;
    }

    /**
     * Sets the table name.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_TABLE}.
     * </p>
     *
     * @param table Table name.
     */
    public void setTable(String table) {
        this.table = table;
    }

    /**
     * Fluent-style version of {@link #setTable(String)}.
     *
     * @param table Table name.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withTable(String table) {
        setTable(table);

        return this;
    }

    /**
     * Returns the host column name (see {@link #setHostColumn(String)}).
     *
     * @return Host column name.
     */
    public String getHostColumn() {
        return hostColumn;
    }

    /**
     * Sets the host column name.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_HOST_COLUMN}.
     * </p>
     *
     * @param hostColumn Host column name.
     */
    public void setHostColumn(String hostColumn) {
        this.hostColumn = hostColumn;
    }

    /**
     * Fluent-style version of {@link #setHostColumn(String)}.
     *
     * @param hostColumn Host column name.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withHostColumn(String hostColumn) {
        setHostColumn(hostColumn);

        return this;
    }

    /**
     * Returns the port column name (see {@link #setPortColumn(String)}).
     *
     * @return Port column name.
     */
    public String getPortColumn() {
        return portColumn;
    }

    /**
     * Sets the port column name.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_PORT_COLUMN}.
     * </p>
     *
     * @param portColumn Port column name.
     */
    public void setPortColumn(String portColumn) {
        this.portColumn = portColumn;
    }

    /**
     * Fluent-style version of {@link #setPortColumn(String)}.
     *
     * @param portColumn Port column name.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withPortColumn(String portColumn) {
        setPortColumn(portColumn);

        return this;
    }

    /**
     * Returns the cluster column name (see {@link #setClusterColumn(String)}).
     *
     * @return Cluster column name.
     */
    public String getClusterColumn() {
        return clusterColumn;
    }

    /**
     * Sets the cluster column name.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_CLUSTER_COLUMN}.
     * </p>
     *
     * @param clusterColumn Cluster column name.
     */
    public void setClusterColumn(String clusterColumn) {
        this.clusterColumn = clusterColumn;
    }

    /**
     * Fluent-style version of {@link #setClusterColumn(String)}.
     *
     * @param clusterColumn Cluster column name.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withClusterColumn(String clusterColumn) {
        setClusterColumn(clusterColumn);

        return this;
    }

    /**
     * Returns the JDBC query timeout (see {@link #setQueryTimeout(int)}).
     *
     * @return Timeout in seconds.
     */
    public int getQueryTimeout() {
        return queryTimeout;
    }

    /**
     * Sets the JDBC query timeout value in seconds.
     *
     * <p>
     * If this parameter is set to a positive value then such timeout will be set via {@link Statement#setQueryTimeout(int)} for all
     * database queries.
     * </p>
     *
     * @param queryTimeout Timeout in seconds.
     */
    public void setQueryTimeout(int queryTimeout) {
        this.queryTimeout = queryTimeout;
    }

    /**
     * Fluent-style version of {@link #setQueryTimeout(int)}.
     *
     * @param queryTimeout Timeout in seconds.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withQueryTimeout(int queryTimeout) {
        setQueryTimeout(queryTimeout);

        return this;
    }

    /**
     * Returns the time interval in milliseconds between stale node cleanup runs (see {@link #setCleanupInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getCleanupInterval() {
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
    public void setCleanupInterval(long cleanupInterval) {
        this.cleanupInterval = cleanupInterval;
    }

    /**
     * Fluent-style version of {@link #setCleanupInterval(long)}.
     *
     * @param cleanupInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public JdbcSeedNodeProviderConfig withCleanupInterval(long cleanupInterval) {
        setCleanupInterval(cleanupInterval);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
