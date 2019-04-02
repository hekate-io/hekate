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

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBC-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provider uses a shared database table to store seed node addresses. When provider  starts discovering other nodes it registers the
 * local node addresses within the database table (with configurable name and columns) and uses this table to search for remote node
 * addresses.
 * </p>
 *
 * <p>Please see the documentation of {@link JdbcSeedNodeProviderConfig} class for more details about available configuration options.</p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class JdbcSeedNodeProvider implements SeedNodeProvider, JmxSupport<JdbcSeedNodeProviderJmx> {
    private static final Logger log = LoggerFactory.getLogger(JdbcSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final DataSource ds;

    private final int queryTimeout;

    private final long cleanupInterval;

    @ToStringIgnore
    private final String selSql;

    @ToStringIgnore
    private final String delSql;

    @ToStringIgnore
    private final String insSql;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public JdbcSeedNodeProvider(JdbcSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "configuration");

        ConfigCheck check = ConfigCheck.get(JdbcSeedNodeProviderConfig.class);

        check.notNull(cfg.getDataSource(), "datasource");
        check.notEmpty(cfg.getTable(), "table");
        check.notEmpty(cfg.getHostColumn(), "host column");
        check.notEmpty(cfg.getPortColumn(), "port column");
        check.notEmpty(cfg.getClusterColumn(), "cluster name column");

        ds = cfg.getDataSource();
        queryTimeout = cfg.getQueryTimeout();
        cleanupInterval = cfg.getCleanupInterval();

        String table = cfg.getTable().trim();
        String host = cfg.getHostColumn().trim();
        String port = cfg.getPortColumn().trim();
        String cluster = cfg.getClusterColumn().trim();

        selSql = "SELECT " + host + ", " + port + " FROM " + table + " WHERE " + cluster + " = ?";
        delSql = "DELETE FROM " + table + " WHERE " + host + " = ? AND " + port + " = ? AND " + cluster + " = ?";
        insSql = "INSERT INTO " + table + " (" + host + ", " + port + ", " + cluster + ") VALUES (?, ?, ?)";
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        try (
            Connection conn = ds.getConnection();
            PreparedStatement st = conn.prepareStatement(selSql)
        ) {
            if (DEBUG) {
                log.debug("Executing SQL query [sql={}, cluster={}]", selSql, cluster);
            }

            if (queryTimeout > 0) {
                st.setQueryTimeout(queryTimeout);
            }

            st.setString(1, cluster);

            try (ResultSet rs = st.executeQuery()) {
                List<InetSocketAddress> result = new ArrayList<>();

                while (rs.next()) {
                    String host = rs.getString(1);
                    int port = rs.getInt(2);

                    InetSocketAddress address = new InetSocketAddress(host, port);

                    result.add(address);
                }

                if (DEBUG) {
                    log.debug("Loaded data from a database [result={}]", result);
                }

                return result;
            }
        } catch (SQLException e) {
            throw new HekateException("Failed to load seed nodes list from a database.", e);
        }
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        doRegister(cluster, node);
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        doUnregister(cluster, node);
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        doRegister(cluster, node);
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        doUnregister(cluster, node);
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    /**
     * Returns the data source of this provider.
     *
     * @return Data source.
     *
     * @see JdbcSeedNodeProviderConfig#setDataSource(DataSource)
     */
    public DataSource dataSource() {
        return ds;
    }

    /**
     * Returns the JDBC query timeout in seconds.
     *
     * @return Timeout in seconds.
     *
     * @see JdbcSeedNodeProviderConfig#setQueryTimeout(int)
     */
    public int queryTimeout() {
        return queryTimeout;
    }

    /**
     * Returns the SQL string for selecting records from the seed nodes table.
     *
     * @return SQL string.
     *
     * @see JdbcSeedNodeProviderConfig#setTable(String)
     */
    public String selectSql() {
        return selSql;
    }

    /**
     * Returns the SQL string for inserting new records into the seed nodes table.
     *
     * @return SQL string.
     *
     * @see JdbcSeedNodeProviderConfig#setTable(String)
     */
    public String deleteSql() {
        return delSql;
    }

    /**
     * Returns the SQL string for deleting records from the seed nodes table.
     *
     * @return SQL string.
     *
     * @see JdbcSeedNodeProviderConfig#setTable(String)
     */
    public String insertSql() {
        return insSql;
    }

    @Override
    public JdbcSeedNodeProviderJmx jmx() {
        return new JdbcSeedNodeProviderJmx() {
            @Override
            public String getDataSourceInfo() {
                return ds.toString();
            }

            @Override
            public int getQueryTimeout() {
                return queryTimeout;
            }

            @Override
            public long getCleanupInterval() {
                return cleanupInterval;
            }

            @Override
            public String getInsertSql() {
                return insSql;
            }

            @Override
            public String getSelectSql() {
                return selSql;
            }

            @Override
            public String getDeleteSql() {
                return delSql;
            }
        };
    }

    private void doUnregister(String cluster, InetSocketAddress address) throws HekateException {
        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement delSt = conn.prepareStatement(delSql)) {
                if (queryTimeout > 0) {
                    delSt.setQueryTimeout(queryTimeout);
                }

                String host = AddressUtils.host(address);
                int port = address.getPort();

                if (DEBUG) {
                    log.debug("Executing SQL query [sql={}, host={}, port={}, cluster={}]", delSql, host, port, cluster);
                }

                delSt.setString(1, host);
                delSt.setInt(2, port);
                delSt.setString(3, cluster);

                int updated = delSt.executeUpdate();

                if (DEBUG) {
                    log.debug("Done executing SQL query [updated-records={}]", updated);
                }

                conn.commit();
            } catch (SQLException e) {
                rollback(conn);

                throw e;
            }
        } catch (SQLException e) {
            throw new HekateException("Failed to register seed node within a database "
                + "[cluster=" + cluster + ", node=" + address + ']', e);
        }
    }

    private void doRegister(String cluster, InetSocketAddress node) throws HekateException {
        InetAddress address = node.getAddress();

        ArgAssert.check(address != null, "Host address can't be resolved [address=" + node + ']');

        try (Connection conn = ds.getConnection()) {
            conn.setAutoCommit(false);

            try (
                PreparedStatement delSt = conn.prepareStatement(delSql);
                PreparedStatement insSt = conn.prepareStatement(insSql)
            ) {
                if (queryTimeout > 0) {
                    delSt.setQueryTimeout(queryTimeout);
                    insSt.setQueryTimeout(queryTimeout);
                }

                String host = address.getHostAddress();
                int port = node.getPort();

                if (DEBUG) {
                    log.debug("Executing SQL query [sql={}, host={}, port={}, cluster={}]", delSql, host, port, cluster);
                }

                delSt.setString(1, host);
                delSt.setInt(2, port);
                delSt.setString(3, cluster);

                delSt.executeUpdate();

                if (DEBUG) {
                    log.debug("Executing SQL query [sql={}, host={}, port={}, cluster={}]", insSql, host, port, cluster);
                }

                insSt.setString(1, host);
                insSt.setInt(2, port);
                insSt.setString(3, cluster);

                insSt.executeUpdate();

                conn.commit();
            } catch (SQLException e) {
                rollback(conn);

                throw e;
            }
        } catch (SQLException e) {
            throw new HekateException("Failed to register seed node [cluster=" + cluster + ", node=" + node + ']', e);
        }
    }

    private void rollback(Connection conn) {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                // No-op.
            }
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
