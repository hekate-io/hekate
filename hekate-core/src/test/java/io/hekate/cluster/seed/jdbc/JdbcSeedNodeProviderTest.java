/*
 * Copyright 2017 The Hekate Project
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

import com.mysql.cj.jdbc.MysqlDataSource;
import io.hekate.HekateTestProps;
import io.hekate.cluster.seed.PersistentSeedNodeProviderCommonTest;
import io.hekate.core.HekateException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.postgresql.ds.PGSimpleDataSource;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class JdbcSeedNodeProviderTest extends PersistentSeedNodeProviderCommonTest<JdbcSeedNodeProvider> {
    private final DataSource ds;

    private Connection keepDbAlive;

    public JdbcSeedNodeProviderTest(DataSource ds) {
        this.ds = ds;
    }

    @Parameters(name = "{index}:{0}")
    public static Collection<DataSource> getDataSources() {
        List<DataSource> dataSources = new ArrayList<>();

        dataSources.add(newH2DataSource());

        // MySQL
        if (HekateTestProps.is("MYSQL_ENABLED")) {
            MysqlDataSource mysql = new MysqlDataSource();

            mysql.setURL(HekateTestProps.get("MYSQL_URL"));
            mysql.setUser(HekateTestProps.get("MYSQL_USER"));
            mysql.setPassword(HekateTestProps.get("MYSQL_PASSWORD"));

            dataSources.add(mysql);
        }

        // PostgreSQL
        if (HekateTestProps.is("POSTGRES_ENABLED")) {
            PGSimpleDataSource postgres = new PGSimpleDataSource();

            postgres.setUrl(HekateTestProps.get("POSTGRES_URL"));
            postgres.setUser(HekateTestProps.get("POSTGRES_USER"));
            postgres.setPassword(HekateTestProps.get("POSTGRES_PASSWORD"));

            dataSources.add(postgres);
        }

        return dataSources;
    }

    public static void initializeDatabase(DataSource ds, JdbcSeedNodeProviderConfig cfg) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            initializeDatabase(conn, cfg);
        }
    }

    public static void initializeDatabase(Connection conn, JdbcSeedNodeProviderConfig cfg) throws SQLException {
        try (
            Statement st = conn.createStatement()
        ) {
            String sql = "CREATE TABLE IF NOT EXISTS " + cfg.getTable() + " ("
                + cfg.getHostColumn() + " VARCHAR(255),"
                + cfg.getPortColumn() + " INT, "
                + cfg.getClusterColumn() + " VARCHAR(255) "
                + ")";

            st.execute(sql);
        }
    }

    public static JdbcDataSource newH2DataSource() {
        JdbcDataSource h2 = new JdbcDataSource();

        h2.setURL("jdbc:h2:mem:test");

        return h2;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        keepDbAlive = datasource().getConnection();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();

        if (keepDbAlive != null) {
            try {
                keepDbAlive.close();
            } finally {
                keepDbAlive = null;
            }
        }
    }

    @Test
    public void testDbErrorOnStopDiscovery() throws Exception {
        BrokenDataSourceMock errDs = new BrokenDataSourceMock(datasource());

        JdbcSeedNodeProvider provider = createProvider(errDs);

        errDs.scheduleErrors(1);

        InetSocketAddress address = newSocketAddress(10001);

        try {
            provider.startDiscovery(CLUSTER_1, address);

            fail();
        } catch (HekateException e) {
            assertTrue("Cause:" + e.getCause(), e.isCausedBy(SQLException.class));
        }

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());

        provider.startDiscovery(CLUSTER_1, address);

        assertEquals(address, provider.findSeedNodes(CLUSTER_1).get(0));

        provider.stopDiscovery(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testDbErrorOnStartDiscovery() throws Exception {
        BrokenDataSourceMock errDs = new BrokenDataSourceMock(datasource());

        JdbcSeedNodeProvider provider = createProvider(errDs);

        InetSocketAddress address = newSocketAddress(10001);

        provider.startDiscovery(CLUSTER_1, address);

        assertFalse(provider.findSeedNodes(CLUSTER_1).isEmpty());
        assertEquals(address, provider.findSeedNodes(CLUSTER_1).get(0));

        errDs.scheduleErrors(1);

        try {
            provider.stopDiscovery(CLUSTER_1, address);

            fail();
        } catch (HekateException e) {
            assertTrue("Cause:" + e.getCause(), e.isCausedBy(SQLException.class));
        }

        assertFalse(provider.findSeedNodes(CLUSTER_1).isEmpty());
        assertEquals(address, provider.findSeedNodes(CLUSTER_1).get(0));

        provider.stopDiscovery(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testDbErrorOnGetNodes() throws Exception {
        BrokenDataSourceMock errDs = new BrokenDataSourceMock(datasource());

        JdbcSeedNodeProvider provider = createProvider(errDs);

        InetSocketAddress address = newSocketAddress(10001);

        provider.startDiscovery(CLUSTER_1, address);

        assertFalse(provider.findSeedNodes(CLUSTER_1).isEmpty());
        assertEquals(address, provider.findSeedNodes(CLUSTER_1).get(0));

        errDs.scheduleErrors(1);

        try {
            provider.findSeedNodes(CLUSTER_1);

            fail();
        } catch (HekateException e) {
            assertTrue("Cause:" + e.getCause(), e.isCausedBy(SQLException.class));
        }

        assertFalse(provider.findSeedNodes(CLUSTER_1).isEmpty());
        assertEquals(address, provider.findSeedNodes(CLUSTER_1).get(0));

        provider.stopDiscovery(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    public DataSource datasource() {
        return ds;
    }

    protected JdbcSeedNodeProviderConfig createConfig() {
        return new JdbcSeedNodeProviderConfig();
    }

    @Override
    protected JdbcSeedNodeProvider createProvider() throws Exception {
        return createProvider(datasource());
    }

    private JdbcSeedNodeProvider createProvider(DataSource ds) throws Exception {
        JdbcSeedNodeProviderConfig cfg = createConfig();

        initializeDatabase(ds, cfg);

        cfg.setDataSource(ds);
        cfg.setCleanupInterval(100);

        return new JdbcSeedNodeProvider(cfg);
    }
}
