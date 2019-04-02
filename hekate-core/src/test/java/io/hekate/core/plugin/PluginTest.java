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

package io.hekate.core.plugin;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.test.HekateTestError;
import io.hekate.test.HekateTestException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class PluginTest extends HekateNodeTestBase {
    public static class NonInstantiableServiceFactory implements ServiceFactory<Service> {
        public NonInstantiableServiceFactory(String ignore) {
            assert ignore != null : "Ignore.";
        }

        @Override
        public Service createService() {
            return new Service() {
                // No-op.
            };
        }
    }

    private static class TestPlugin implements Plugin {
        private final AtomicInteger installed = new AtomicInteger();

        private final AtomicInteger started = new AtomicInteger();

        private final AtomicInteger stopped = new AtomicInteger();

        @Override
        public void install(HekateBootstrap boot) {
            installed.incrementAndGet();
        }

        @Override
        public void start(Hekate hekate) throws HekateException {
            started.incrementAndGet();
        }

        @Override
        public void stop() throws HekateException {
            stopped.incrementAndGet();
        }

        public int getInstalled() {
            return installed.get();
        }

        public int getStarted() {
            return started.get();
        }

        public int getStopped() {
            return stopped.get();
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        TestPlugin act1 = new TestPlugin();
        TestPlugin act2 = new TestPlugin();

        HekateTestNode node = createNodeWithPlugin(act1, act2);

        node.join();

        assertEquals(1, act1.getInstalled());
        assertEquals(1, act1.getStarted());

        assertEquals(1, act2.getInstalled());
        assertEquals(1, act2.getStarted());

        node.leave();

        assertEquals(1, act1.getInstalled());
        assertEquals(1, act1.getStarted());
        assertEquals(1, act1.getStopped());

        assertEquals(1, act2.getInstalled());
        assertEquals(1, act2.getStarted());
        assertEquals(1, act2.getStopped());
    }

    @Test
    public void testErrorOnPrepare() throws Exception {
        TestPlugin act1 = new TestPlugin();
        TestPlugin act2 = new TestPlugin() {
            @Override
            public void install(HekateBootstrap boot) {
                super.install(boot);

                throw new HekateConfigurationException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin act3 = new TestPlugin();

        try {
            createNodeWithPlugin(act1, act2, act3);

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(HekateTestError.MESSAGE, e.getMessage());
        }

        assertEquals(1, act1.getInstalled());
        assertEquals(0, act1.getStarted());
        assertEquals(0, act1.getStopped());

        assertEquals(1, act2.getInstalled());
        assertEquals(0, act2.getStarted());
        assertEquals(0, act2.getStopped());

        assertEquals(0, act3.getInstalled());
        assertEquals(0, act3.getStarted());
        assertEquals(0, act3.getStopped());

    }

    @Test
    public void testErrorOnStart() throws Exception {
        TestPlugin act1 = new TestPlugin();
        TestPlugin act2 = new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                super.start(hekate);

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin act3 = new TestPlugin();

        HekateTestNode node = createNodeWithPlugin(act1, act2, act3);

        repeat(3, i -> {
            try {
                node.join();

                fail("Error was expected.");
            } catch (HekateFutureException e) {
                assertEquals(HekateTestError.MESSAGE, e.getCause().getMessage());
            }

            assertSame(Hekate.State.DOWN, node.state());

            assertEquals(1, act1.getInstalled());
            assertEquals(i + 1, act1.getStarted());
            assertEquals(i + 1, act1.getStopped());

            assertEquals(1, act2.getInstalled());
            assertEquals(i + 1, act2.getStarted());
            assertEquals(i + 1, act2.getStopped());

            assertEquals(1, act3.getInstalled());
            assertEquals(0, act3.getStarted());
            assertEquals(0, act3.getStopped());
        });
    }

    @Test
    public void testErrorOnStop() throws Exception {
        TestPlugin act1 = new TestPlugin();
        TestPlugin act2 = new TestPlugin() {
            @Override
            public void stop() throws HekateException {
                super.stop();

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };
        TestPlugin act3 = new TestPlugin();

        HekateTestNode node = createNodeWithPlugin(act1, act2, act3);

        repeat(3, i -> {
            node.join();
            node.leave();

            assertSame(Hekate.State.DOWN, node.state());

            assertEquals(1, act1.getInstalled());
            assertEquals(i + 1, act1.getStarted());
            assertEquals(i + 1, act1.getStopped());

            assertEquals(1, act2.getInstalled());
            assertEquals(i + 1, act2.getStarted());
            assertEquals(i + 1, act2.getStopped());

            assertEquals(1, act3.getInstalled());
            assertEquals(i + 1, act3.getStarted());
            assertEquals(i + 1, act3.getStopped());
        });
    }

    @Test
    public void testAddRoleAndProperty() throws Exception {
        String role = UUID.randomUUID().toString();
        String propKey = UUID.randomUUID().toString();
        String propVal = UUID.randomUUID().toString();

        Hekate node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void install(HekateBootstrap boot) {
                boot.withRole(role);
                boot.withProperty(propKey, propVal);
            }
        }).join();

        assertTrue(node.localNode().hasRole(role));
        assertEquals(propVal, node.localNode().property(propKey));
    }

    @Test
    public void testConfigureServiceFactory() throws Exception {
        HekateTestNode node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void install(HekateBootstrap boot) {
                // Check that service factory is not registered yet.
                assertFalse(boot.service(MessagingServiceFactory.class).isPresent());

                // Should register new service factory.
                boot.withService(MessagingServiceFactory.class, Assert::assertNotNull);

                // Check that service factory is now available.
                assertTrue(boot.service(MessagingServiceFactory.class).isPresent());

                // Check that specifying a non-instantiable service factory would throw an error.
                expect(HekateConfigurationException.class, () -> boot.withService(NonInstantiableServiceFactory.class, f ->
                    fail())
                );
            }
        });

        node.join();
    }

    @Test
    public void testAccessService() throws Exception {
        AtomicBoolean success = new AtomicBoolean();

        HekateTestNode node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                assertNotNull(hekate.network());

                success.set(true);
            }
        });

        node.join();

        assertTrue(success.get());
    }

    @Test
    public void testAccessNode() throws Exception {
        AtomicBoolean success = new AtomicBoolean();

        HekateTestNode node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                assertNotNull(hekate);

                success.set(true);
            }
        });

        node.join();

        assertTrue(success.get());
    }

    @Test
    public void testLeaveFromPlugin() throws Exception {
        AtomicBoolean success = new AtomicBoolean();

        HekateTestNode node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                hekate.leaveAsync();

                success.set(true);
            }
        });

        node.join();

        assertTrue(success.get());

        assertSame(Hekate.State.DOWN, node.state());
    }

    @Test
    public void testTerminateFromPlugin() throws Exception {
        AtomicBoolean success = new AtomicBoolean();

        HekateTestNode node = createNodeWithPlugin(new TestPlugin() {
            @Override
            public void start(Hekate hekate) throws HekateException {
                hekate.terminateAsync();

                success.set(true);
            }
        });

        node.join();

        assertTrue(success.get());

        assertSame(Hekate.State.DOWN, node.state());
    }

    private HekateTestNode createNodeWithPlugin(TestPlugin... plugin) throws Exception {
        return createNode(c -> {
            for (TestPlugin act : plugin) {
                c.withPlugin(act);
            }
        });
    }
}
