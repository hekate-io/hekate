/*
 * Copyright 2022 The Hekate Project
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
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
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
public class JdbcSeedNodeProvider implements SeedNodeProvider, JmxSupport<JdbcSeedNodeProviderJmx>, ConfigReportSupport {
    private interface TxTask {
        void execute(Connection conn) throws SQLException;
    }

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
        check.notEmpty(cfg.getNamespaceColumn(), "namespace column");

        ds = cfg.getDataSource();
        queryTimeout = cfg.getQueryTimeout();
        cleanupInterval = cfg.getCleanupInterval();

        String table = cfg.getTable().trim();
        String host = cfg.getHostColumn().trim();
        String port = cfg.getPortColumn().trim();
        String namespace = cfg.getNamespaceColumn().trim();

        selSql = "SELECT " + host + ", " + port + " FROM " + table + " WHERE " + namespace + " = ?";
        delSql = "DELETE FROM " + table + " WHERE " + host + " = ? AND " + port + " = ? AND " + namespace + " = ?";
        insSql = "INSERT INTO " + table + " (" + host + ", " + port + ", " + namespace + ") VALUES (?, ?, ?)";
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("jdbc", r -> {
            r.value("datasource", ds);
            r.value("insert-sql", insSql);
            r.value("cleanup-interval", cleanupInterval);
            r.value("query-timeout", queryTimeout);
        });
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        try (
            Connection conn = ds.getConnection();
            PreparedStatement st = conn.prepareStatement(selSql)
        ) {
            if (DEBUG) {
                log.debug("Executing SQL query [sql={}, namespace={}]", selSql, namespace);
            }

            if (queryTimeout > 0) {
                st.setQueryTimeout(queryTimeout);
            }

            st.setString(1, namespace);

            try (ResultSet rs = st.executeQuery()) {
                List<InetSocketAddress> result = new ArrayList<>();

                while (rs.next()) {
                    String host = rs.getString(1);
                    int port = rs.getInt(2);

                    result.add(new InetSocketAddress(host, port));
                }

                if (DEBUG) {
                    log.debug("Loaded data from database [result={}]", result);
                }

                return result;
            }
        } catch (SQLException e) {
            throw new HekateException("Failed to load seed nodes list from a database.", e);
        }
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        doRegister(namespace, node);
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        doUnregister(namespace, node);
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        doRegister(namespace, node);
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
        doUnregister(namespace, node);
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

    private void doUnregister(String namespace, InetSocketAddress address) throws HekateException {
        try {
            runWithTx(conn -> {
                try (PreparedStatement delSt = conn.prepareStatement(delSql)) {
                    if (queryTimeout > 0) {
                        delSt.setQueryTimeout(queryTimeout);
                    }

                    String host = AddressUtils.host(address);
                    int port = address.getPort();

                    if (DEBUG) {
                        log.debug("Executing SQL query [sql={}, host={}, port={}, namespace={}]", delSql, host, port, namespace);
                    }

                    delSt.setString(1, host);
                    delSt.setInt(2, port);
                    delSt.setString(3, namespace);

                    int deleted = delSt.executeUpdate();

                    if (DEBUG) {
                        log.debug("Done executing SQL delete query [deleted-records={}]", deleted);
                    }
                }
            });
        } catch (SQLException e) {
            throw new HekateException("Failed to register seed node within a database "
                + "[namespace=" + namespace + ", node=" + address + ']', e);
        }
    }

    private void doRegister(String namespace, InetSocketAddress node) throws HekateException {
        try {
            runWithTx(conn -> {
                try (
                    PreparedStatement delSt = conn.prepareStatement(delSql);
                    PreparedStatement insSt = conn.prepareStatement(insSql)
                ) {
                    if (queryTimeout > 0) {
                        delSt.setQueryTimeout(queryTimeout);
                        insSt.setQueryTimeout(queryTimeout);
                    }

                    String host = node.getAddress().getHostAddress();
                    int port = node.getPort();

                    if (DEBUG) {
                        log.debug("Executing SQL query [sql={}, host={}, port={}, namespace={}]", delSql, host, port, namespace);
                    }

                    delSt.setString(1, host);
                    delSt.setInt(2, port);
                    delSt.setString(3, namespace);

                    int deleted = delSt.executeUpdate();

                    if (DEBUG) {
                        log.debug("Done executing SQL delete query [deleted-records={}]", deleted);
                    }

                    if (DEBUG) {
                        log.debug("Executing SQL query [sql={}, host={}, port={}, namespace={}]", insSql, host, port, namespace);
                    }

                    insSt.setString(1, host);
                    insSt.setInt(2, port);
                    insSt.setString(3, namespace);

                    insSt.executeUpdate();

                    if (DEBUG) {
                        log.debug("Done executing SQL insert query.");
                    }
                }
            });
        } catch (SQLException e) {
            throw new HekateException("Failed to register seed node [namespace=" + namespace + ", node=" + node + ']', e);
        }
    }

    private void runWithTx(TxTask task) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            boolean oldAutoCommit = conn.getAutoCommit();

            conn.setAutoCommit(false);

            try {
                task.execute(conn);

                conn.commit();
            } catch (Exception e) {
                rollback(conn);

                throw e;
            } finally {
                resetAutoCommit(oldAutoCommit, conn);
            }
        }
    }

    private void rollback(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException e) {
            // No-op.
        }
    }

    private void resetAutoCommit(boolean oldAutoCommit, Connection conn) {
        try {
            conn.setAutoCommit(oldAutoCommit);
        } catch (SQLException e) {
            // No-op.
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
