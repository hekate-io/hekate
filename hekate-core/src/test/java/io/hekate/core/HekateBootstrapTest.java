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

package io.hekate.core;

import io.hekate.HekateTestBase;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.JavaCodecFactory;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
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
    public void testNodeName() {
        assertNull(bootstrap.getNodeName());

        bootstrap.setNodeName("test");

        assertEquals("test", bootstrap.getNodeName());

        assertEquals("test2", bootstrap.withNodeName("test2").getNodeName());
    }

    @Test
    public void testClusterName() {
        assertEquals(HekateBootstrap.DEFAULT_CLUSTER_NAME, bootstrap.getClusterName());

        bootstrap.setClusterName("test");

        assertEquals("test", bootstrap.getClusterName());

        assertEquals("test2", bootstrap.withClusterName("test2").getClusterName());
    }

    @Test
    public void testNodeRoles() {
        assertNull(bootstrap.getNodeRoles());

        bootstrap.setNodeRoles(new HashSet<>(Arrays.asList("role1", "role2")));

        assertNotNull(bootstrap.getNodeRoles());
        assertTrue(bootstrap.getNodeRoles().contains("role1"));
        assertTrue(bootstrap.getNodeRoles().contains("role2"));

        bootstrap.withNodeRole("role3");

        assertTrue(bootstrap.getNodeRoles().contains("role3"));

        bootstrap.setNodeRoles(null);

        assertNull(bootstrap.getNodeRoles());

        bootstrap.withNodeRole("role3");

        assertNotNull(bootstrap.getNodeRoles());
        assertTrue(bootstrap.getNodeRoles().contains("role3"));

        assertTrue(bootstrap.withNodeRole("role4").getNodeRoles().contains("role4"));
    }

    @Test
    public void testNodeProperties() {
        assertNull(bootstrap.getNodeProperties());

        bootstrap.setNodeProperties(Stream.of("prop1", "prop2").collect(toMap(Function.identity(), Function.identity())));

        assertNotNull(bootstrap.getNodeProperties());
        assertTrue(bootstrap.getNodeProperties().containsKey("prop1"));
        assertTrue(bootstrap.getNodeProperties().containsKey("prop2"));

        bootstrap.withNodeProperty("prop3", "prop3");

        assertTrue(bootstrap.getNodeProperties().containsKey("prop3"));

        bootstrap.setNodeProperties(null);

        assertNull(bootstrap.getNodeProperties());

        bootstrap.withNodeProperty("prop3", "prop3");

        assertNotNull(bootstrap.getNodeProperties());
        assertTrue(bootstrap.getNodeProperties().containsKey("prop3"));

        assertTrue(bootstrap.withNodeProperty("prop4", "prop4").getNodeProperties().containsKey("prop4"));
    }

    @Test
    public void testNodePropertyProviders() {
        NodePropertyProvider p1 = mock(NodePropertyProvider.class);
        NodePropertyProvider p2 = mock(NodePropertyProvider.class);

        assertNull(bootstrap.getNodePropertyProviders());

        bootstrap.setNodePropertyProviders(Arrays.asList(p1, p2));

        assertEquals(2, bootstrap.getNodePropertyProviders().size());
        assertTrue(bootstrap.getNodePropertyProviders().contains(p1));
        assertTrue(bootstrap.getNodePropertyProviders().contains(p2));

        bootstrap.setNodePropertyProviders(null);

        assertNull(bootstrap.getNodePropertyProviders());

        assertSame(bootstrap, bootstrap.withNodePropertyProvider(p1));

        assertEquals(1, bootstrap.getNodePropertyProviders().size());
        assertTrue(bootstrap.getNodePropertyProviders().contains(p1));
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

        CodecFactory<Object> factory1 = new JavaCodecFactory<>();
        CodecFactory<Object> factory2 = new JavaCodecFactory<>();

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
        List<Hekate> instances = new LinkedList<>();

        try {
            repeat(3, i -> instances.add(new HekateBootstrap().join()));
        } finally {
            for (Hekate instance : instances) {
                try {
                    instance.leave();
                } catch (InterruptedException e) {
                    // Ignore.
                }
            }
        }
    }

    @Test
    public void testJoinWithDefaultsAsync() throws Exception {
        List<LeaveFuture> leave = new LinkedList<>();

        try {
            List<JoinFuture> joins = new LinkedList<>();

            repeat(3, i -> {
                HekateBootstrap boot = new HekateBootstrap();

                joins.add(boot.joinAsync());
            });

            for (JoinFuture instance : joins) {
                leave.add(instance.get().leaveAsync());
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
