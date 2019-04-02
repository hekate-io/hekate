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

import io.hekate.HekateTestBase;
import javax.sql.DataSource;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class JdbcSeedNodeProviderConfigTest extends HekateTestBase {
    private final JdbcSeedNodeProviderConfig cfg = new JdbcSeedNodeProviderConfig();

    @Test
    public void testTable() {
        assertEquals(JdbcSeedNodeProviderConfig.DEFAULT_TABLE, cfg.getTable());

        cfg.setTable("custom_table");

        assertEquals("custom_table", cfg.getTable());

        assertSame(cfg, cfg.withTable("custom_table2"));

        assertEquals("custom_table2", cfg.getTable());
    }

    @Test
    public void testClusterColumn() {
        assertEquals(JdbcSeedNodeProviderConfig.DEFAULT_CLUSTER_COLUMN, cfg.getClusterColumn());

        cfg.setClusterColumn("custom_cluster");

        assertEquals("custom_cluster", cfg.getClusterColumn());

        assertSame(cfg, cfg.withClusterColumn("custom_cluster2"));

        assertEquals("custom_cluster2", cfg.getClusterColumn());
    }

    @Test
    public void testHostColumn() {
        assertEquals(JdbcSeedNodeProviderConfig.DEFAULT_HOST_COLUMN, cfg.getHostColumn());

        cfg.setHostColumn("custom_host");

        assertEquals("custom_host", cfg.getHostColumn());

        assertSame(cfg, cfg.withHostColumn("custom_host2"));

        assertEquals("custom_host2", cfg.getHostColumn());
    }

    @Test
    public void testPortColumn() {
        assertEquals(JdbcSeedNodeProviderConfig.DEFAULT_PORT_COLUMN, cfg.getPortColumn());

        cfg.setPortColumn("custom_port");

        assertEquals("custom_port", cfg.getPortColumn());

        assertSame(cfg, cfg.withPortColumn("custom_port2"));

        assertEquals("custom_port2", cfg.getPortColumn());
    }

    @Test
    public void testCleanupInterval() {
        assertEquals(JdbcSeedNodeProviderConfig.DEFAULT_CLEANUP_INTERVAL, cfg.getCleanupInterval());

        cfg.setCleanupInterval(10001);

        assertEquals(10001, cfg.getCleanupInterval());

        assertSame(cfg, cfg.withCleanupInterval(10002));

        assertEquals(10002, cfg.getCleanupInterval());
    }

    @Test
    public void testQueryTimeout() {
        assertEquals(0, cfg.getQueryTimeout());

        cfg.setQueryTimeout(10001);

        assertEquals(10001, cfg.getQueryTimeout());

        assertSame(cfg, cfg.withQueryTimeout(10002));

        assertEquals(10002, cfg.getQueryTimeout());
    }

    @Test
    public void testDataSource() {
        DataSource ds1 = mock(DataSource.class);
        DataSource ds2 = mock(DataSource.class);

        assertNull(cfg.getDataSource());

        cfg.setDataSource(ds1);

        assertSame(ds1, cfg.getDataSource());

        assertSame(cfg, cfg.withDataSource(ds2));

        assertSame(ds2, cfg.getDataSource());
    }
}
