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

package io.hekate.core;

import io.hekate.HekateTestBase;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.Test;

import static java.util.stream.Collectors.toMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class HekateBootstrapTest extends HekateTestBase {
    private static class DummyService implements Service {
        // No-op.
    }

    private final HekateBootstrap bootstrap = new HekateBootstrap();

    @Test
    public void testName() {
        assertNull(bootstrap.getNodeName());

        bootstrap.setNodeName("test");

        assertEquals("test", bootstrap.getNodeName());

        assertEquals("test2", bootstrap.withNodeName("test2").getNodeName());
    }

    @Test
    public void testCluster() {
        assertEquals(HekateBootstrap.DEFAULT_CLUSTER_NAME, bootstrap.getClusterName());

        bootstrap.setClusterName("test");

        assertEquals("test", bootstrap.getClusterName());

        assertEquals("test2", bootstrap.withClusterName("test2").getClusterName());
    }

    @Test
    public void testRoles() {
        assertNull(bootstrap.getRoles());

        bootstrap.setRoles(new ArrayList<>(Arrays.asList("role1", "role2")));

        assertNotNull(bootstrap.getRoles());
        assertTrue(bootstrap.getRoles().contains("role1"));
        assertTrue(bootstrap.getRoles().contains("role2"));

        bootstrap.withRole("role3");

        assertTrue(bootstrap.getRoles().contains("role3"));

        bootstrap.setRoles(null);

        assertNull(bootstrap.getRoles());

        bootstrap.withRole("role3");

        assertNotNull(bootstrap.getRoles());
        assertTrue(bootstrap.getRoles().contains("role3"));

        assertTrue(bootstrap.withRole("role4").getRoles().contains("role4"));
    }

    @Test
    public void testProperties() {
        assertNull(bootstrap.getProperties());

        bootstrap.setProperties(Stream.of("prop1", "prop2").collect(toMap(Function.identity(), Function.identity())));

        assertNotNull(bootstrap.getProperties());
        assertTrue(bootstrap.getProperties().containsKey("prop1"));
        assertTrue(bootstrap.getProperties().containsKey("prop2"));

        bootstrap.withProperty("prop3", "prop3");

        assertTrue(bootstrap.getProperties().containsKey("prop3"));

        bootstrap.setProperties(null);

        assertNull(bootstrap.getProperties());

        bootstrap.withProperty("prop3", "prop3");

        assertNotNull(bootstrap.getProperties());
        assertTrue(bootstrap.getProperties().containsKey("prop3"));

        assertTrue(bootstrap.withProperty("prop4", "prop4").getProperties().containsKey("prop4"));
    }

    @Test
    public void testPropertyProviders() {
        PropertyProvider p1 = mock(PropertyProvider.class);
        PropertyProvider p2 = mock(PropertyProvider.class);

        assertNull(bootstrap.getPropertyProviders());

        bootstrap.setPropertyProviders(Arrays.asList(p1, p2));

        assertEquals(2, bootstrap.getPropertyProviders().size());
        assertTrue(bootstrap.getPropertyProviders().contains(p1));
        assertTrue(bootstrap.getPropertyProviders().contains(p2));

        bootstrap.setPropertyProviders(null);

        assertNull(bootstrap.getPropertyProviders());

        assertSame(bootstrap, bootstrap.withPropertyProvider(p1));

        assertEquals(1, bootstrap.getPropertyProviders().size());
        assertTrue(bootstrap.getPropertyProviders().contains(p1));
    }

    @Test
    public void testServices() {
        assertNull(bootstrap.getServices());

        ServiceFactory<DummyService> factory1 = DummyService::new;

        bootstrap.setServices(Collections.singletonList(factory1));

        assertNotNull(bootstrap.getServices());
        assertTrue(bootstrap.getServices().contains(factory1));

        bootstrap.setServices(null);

        assertNull(bootstrap.getServices());

        assertTrue(bootstrap.withService(factory1).getServices().contains(factory1));
    }

    @Test
    public void testDefaultCodec() throws Exception {
        assertNotNull(bootstrap.getDefaultCodec());

        CodecFactory<Object> factory1 = new JdkCodecFactory<>();
        CodecFactory<Object> factory2 = new JdkCodecFactory<>();

        bootstrap.setDefaultCodec(factory1);

        assertSame(factory1, bootstrap.getDefaultCodec());

        assertSame(bootstrap, bootstrap.withDefaultCodec(factory2));

        assertSame(factory2, bootstrap.getDefaultCodec());
    }

    @Test
    public void testPlugins() {
        assertNull(bootstrap.getPlugins());

        Plugin p = mock(Plugin.class);

        bootstrap.setPlugins(Collections.singletonList(p));

        assertNotNull(bootstrap.getPlugins());
        assertTrue(bootstrap.getPlugins().contains(p));

        bootstrap.setPlugins(null);

        assertNull(bootstrap.getPlugins());

        assertTrue(bootstrap.withPlugin(p).getPlugins().contains(p));

        bootstrap.setPlugins(null);

        assertNull(bootstrap.getPlugins());
    }

    @Test
    public void testLifecycleListeners() {
        assertNull(bootstrap.getLifecycleListeners());

        Hekate.LifecycleListener listener = mock(Hekate.LifecycleListener.class);

        bootstrap.setLifecycleListeners(Collections.singletonList(listener));

        assertNotNull(bootstrap.getLifecycleListeners());
        assertTrue(bootstrap.getLifecycleListeners().contains(listener));

        bootstrap.setLifecycleListeners(null);

        assertNull(bootstrap.getLifecycleListeners());

        assertTrue(bootstrap.withLifecycleListener(listener).getLifecycleListeners().contains(listener));

        bootstrap.setLifecycleListeners(null);

        assertNull(bootstrap.getLifecycleListeners());
    }

    @Test
    public void testJoinWithDefaults() throws Exception {
        List<Hekate> nodes = new ArrayList<>();

        try {
            repeat(3, i -> {
                Hekate node = new HekateBootstrap().join();

                assertNotNull(node.cluster());
                assertNotNull(node.network());
                assertNotNull(node.messaging());
                assertNotNull(node.locks());
                assertNotNull(node.coordination());
                assertNotNull(node.election());
                assertNotNull(node.codec());

                nodes.add(node);
            });
        } finally {
            for (Hekate node : nodes) {
                try {
                    node.leave();
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }

    @Test
    public void testJoinWithDefaultsAsync() throws Exception {
        List<LeaveFuture> leave = new ArrayList<>();

        try {
            List<JoinFuture> joins = new ArrayList<>();

            repeat(3, i -> {
                HekateBootstrap boot = new HekateBootstrap();

                joins.add(boot.joinAsync());
            });

            for (JoinFuture future : joins) {
                leave.add(future.get().leaveAsync());
            }
        } finally {
            for (LeaveFuture future : leave) {
                try {
                    get(future);
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }

    @Test
    public void testToString() {
        assertTrue(bootstrap.toString(), bootstrap.toString().startsWith(HekateBootstrap.class.getSimpleName()));
    }
}
