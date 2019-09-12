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

package io.hekate.core.internal;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateException;
import io.hekate.core.plugin.Plugin;
import io.hekate.test.HekateTestError;
import io.hekate.test.HekateTestException;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PluginManagerTest extends HekateTestBase {
    private static class TestPlugin implements Plugin {
        private int prepared;

        private int started;

        private int stopped;

        @Override
        public void install(HekateBootstrap boot) {
            prepared++;

            assertNotNull(boot);
        }

        @Override
        public void start(Hekate hekate) throws HekateException {
            started++;

            assertNotNull(hekate);
        }

        @Override
        public void stop() throws HekateException {
            stopped++;
        }

        public int getPrepared() {
            return prepared;
        }

        public int getStarted() {
            return started;
        }

        public int getStopped() {
            return stopped;
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        TestPlugin plugin1 = new TestPlugin();
        TestPlugin plugin2 = new TestPlugin();

        PluginManager mgr = createManager(plugin1, plugin2);

        mgr.install();

        mgr.start(mock(Hekate.class) /* <- should be safe. */);

        mgr.stop();

        assertEquals(1, plugin1.getPrepared());
        assertEquals(1, plugin1.getStarted());
        assertEquals(1, plugin1.getStopped());

        assertEquals(1, plugin2.getPrepared());
        assertEquals(1, plugin2.getStarted());
        assertEquals(1, plugin2.getStopped());
    }

    @Test
    public void testErrorOnPrepare() throws Exception {
        TestPlugin plugin1 = new TestPlugin();
        TestPlugin plugin2 = new TestPlugin() {
            @Override
            public void install(HekateBootstrap boot) {
                super.install(boot);

                throw new HekateConfigurationException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin plugin3 = new TestPlugin();

        PluginManager mgr = createManager(plugin1, plugin2, plugin3);

        try {
            mgr.install();

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(HekateTestError.MESSAGE, e.getMessage());
        }

        mgr.stop();

        assertEquals(1, plugin1.getPrepared());
        assertEquals(0, plugin1.getStarted());
        assertEquals(0, plugin1.getStopped());

        assertEquals(1, plugin2.getPrepared());
        assertEquals(0, plugin2.getStarted());
        assertEquals(0, plugin2.getStopped());

        assertEquals(0, plugin3.getPrepared());
        assertEquals(0, plugin3.getStarted());
        assertEquals(0, plugin3.getStopped());
    }

    @Test
    public void testErrorOnStart() throws Exception {
        TestPlugin plugin1 = new TestPlugin();
        TestPlugin plugin2 = new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                super.start(hekate);

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin plugin3 = new TestPlugin();

        PluginManager mgr = createManager(plugin1, plugin2, plugin3);

        mgr.install();

        try {
            mgr.start(mock(Hekate.class) /* <- should be safe. */);

            fail("Error was expected.");
        } catch (HekateException e) {
            assertEquals(HekateTestError.MESSAGE, e.getMessage());
        }

        mgr.stop();

        assertEquals(1, plugin1.getPrepared());
        assertEquals(1, plugin1.getStarted());
        assertEquals(1, plugin1.getStopped());

        assertEquals(1, plugin2.getPrepared());
        assertEquals(1, plugin2.getStarted());
        assertEquals(1, plugin2.getStopped());

        assertEquals(1, plugin3.getPrepared());
        assertEquals(0, plugin3.getStarted());
        assertEquals(0, plugin3.getStopped());
    }

    @Test
    public void testErrorOnStop() throws Exception {
        TestPlugin plugin1 = new TestPlugin();
        TestPlugin plugin2 = new TestPlugin() {
            @Override
            public void stop() throws HekateException {
                super.stop();

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin plugin3 = new TestPlugin();

        PluginManager mgr = createManager(plugin1, plugin2, plugin3);

        mgr.install();

        mgr.start(mock(Hekate.class) /* <- should be safe. */);

        mgr.stop();

        assertEquals(1, plugin1.getPrepared());
        assertEquals(1, plugin1.getStarted());
        assertEquals(1, plugin1.getStopped());

        assertEquals(1, plugin2.getPrepared());
        assertEquals(1, plugin2.getStarted());
        assertEquals(1, plugin2.getStopped());

        assertEquals(1, plugin3.getPrepared());
        assertEquals(1, plugin3.getStarted());
        assertEquals(1, plugin3.getStopped());
    }

    @Test
    public void testChangeConfiguration() throws Exception {
        TestPlugin plugin = new TestPlugin() {
            @Override
            public void install(HekateBootstrap boot) {
                boot.withRole("test_role");
                boot.withProperty("test_prop", "test_val");
            }
        };

        PluginManager mgr = createManager(plugin);

        mgr.install();

        assertTrue(mgr.bootstrap().getRoles().contains("test_role"));
        assertEquals("test_val", mgr.bootstrap().getProperties().get("test_prop"));
    }

    private PluginManager createManager(Plugin... plugins) {
        HekateBootstrap boot = new HekateBootstrap();

        boot.setPlugins(Arrays.asList(plugins));

        return new PluginManager(boot);
    }
}
