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

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.test.JdbcTestDataSources;
import java.sql.Connection;
import javax.management.ObjectName;
import javax.sql.DataSource;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JdbcSeedNodeProviderJmxTest extends HekateNodeTestBase {

    private Connection keepDbAlive;

    private JdbcSeedNodeProviderConfig cfg;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        DataSource ds = JdbcTestDataSources.h2();

        cfg = new JdbcSeedNodeProviderConfig();

        cfg.setDataSource(ds);
        cfg.setCleanupInterval(100);
        cfg.setQueryTimeout(1000);

        keepDbAlive = ds.getConnection();

        JdbcSeedNodeProviderTest.initializeDatabase(ds, cfg);
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
    public void test() throws Exception {
        JdbcSeedNodeProvider seedNodeProvider = new JdbcSeedNodeProvider(cfg);

        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(ClusterServiceFactory.class, cluster ->
                cluster.withSeedNodeProvider(seedNodeProvider)
            );
        }).join();

        ObjectName name = node.get(JmxService.class).nameFor(JdbcSeedNodeProviderJmx.class);

        assertEquals(cfg.getDataSource().toString(), jmxAttribute(name, "DataSourceInfo", String.class, node));
        assertEquals(100, (long)jmxAttribute(name, "CleanupInterval", Long.class, node));
        assertEquals(1000, (int)jmxAttribute(name, "QueryTimeout", Integer.class, node));
        assertTrue(((String)jmxAttribute(name, "InsertSql", String.class, node)).contains("INSERT"));
        assertTrue(((String)jmxAttribute(name, "DeleteSql", String.class, node)).contains("DELETE"));
        assertTrue(((String)jmxAttribute(name, "SelectSql", String.class, node)).contains("SELECT"));
    }
}
