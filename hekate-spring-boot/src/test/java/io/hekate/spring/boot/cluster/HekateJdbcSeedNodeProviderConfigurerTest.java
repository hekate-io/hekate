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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.internal.DefaultClusterService;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProvider;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProviderConfig;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProviderTest;
import io.hekate.spring.boot.EnableHekate;
import io.hekate.spring.boot.HekateAutoConfigurerTestBase;
import io.hekate.spring.boot.HekateTestConfigBase;
import io.hekate.test.JdbcTestDataSources;
import java.sql.Connection;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class HekateJdbcSeedNodeProviderConfigurerTest extends HekateAutoConfigurerTestBase {
    @EnableHekate
    @EnableAutoConfiguration
    public static class JdbcEnabledConfig {
        @Bean
        public DataSource dataSource() {
            return JdbcTestDataSources.h2();
        }
    }

    @EnableAutoConfiguration
    public static class JdbcDisabledConfig extends HekateTestConfigBase {
        // No-op.
    }

    private Connection keepDbAlive;

    @Before
    public void setUp() throws Exception {
        keepDbAlive = JdbcTestDataSources.h2().getConnection();

        JdbcSeedNodeProviderTest.initializeDatabase(keepDbAlive, new JdbcSeedNodeProviderConfig());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        if (keepDbAlive != null) {
            keepDbAlive = null;
        }

        super.tearDown();
    }

    @Test
    public void testEnabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.jdbc.enable=true"
        }, JdbcEnabledConfig.class);

        SeedNodeProviderGroup group = (SeedNodeProviderGroup)getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertEquals(1, group.allProviders().size());
        assertTrue(group.allProviders().get(0) instanceof JdbcSeedNodeProvider);
        assertEquals(group.allProviders(), group.liveProviders());
    }

    @Test
    public void testDisabled() throws Exception {
        registerAndRefresh(new String[]{
            "hekate.cluster.seed.jdbc.enable=false"
        }, JdbcDisabledConfig.class);

        SeedNodeProvider provider = getNode().get(DefaultClusterService.class).seedNodeProvider();

        assertFalse(provider instanceof JdbcSeedNodeProvider);
    }
}
