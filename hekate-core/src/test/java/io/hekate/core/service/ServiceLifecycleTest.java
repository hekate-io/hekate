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

package io.hekate.core.service;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.internal.HekateTestNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ServiceLifecycleTest extends HekateNodeTestBase {
    private interface ServiceBase extends Service {
        int getConfigured();

        int getInitialized();

        int getTerminated();
    }

    private interface ServiceA extends ServiceBase {
        // No-op.
    }

    private interface ServiceB extends ServiceBase {
        // No-op.
    }

    private static class TestServiceBase implements InitializingService, TerminatingService, ConfigurableService {
        private final AtomicInteger configured = new AtomicInteger();

        private final AtomicInteger initialized = new AtomicInteger();

        private final AtomicInteger terminated = new AtomicInteger();

        private final Set<String> initRoles;

        private final Map<String, String> initProps;

        private final Map<String, List<String>> initServiceProps;

        public TestServiceBase(Set<String> initRoles, Map<String, String> initProps, Map<String, List<String>> initServiceProps) {
            this.initRoles = initRoles;
            this.initProps = initProps;
            this.initServiceProps = initServiceProps;
        }

        @Override
        public void configure(ConfigurationContext ctx) {
            configured.incrementAndGet();

            if (initRoles != null) {
                initRoles.forEach(ctx::addNodeRole);
            }

            if (initProps != null) {
                initProps.forEach(ctx::addNodeProperty);
            }

            if (initServiceProps != null) {
                initServiceProps.forEach((name, values) -> values.forEach(val -> ctx.addServiceProperty(name, val)));
            }
        }

        @Override
        public void initialize(InitializationContext ctx) throws HekateException {
            initialized.incrementAndGet();
        }

        @Override
        public void terminate() throws HekateException {
            terminated.incrementAndGet();
        }

        public int getConfigured() {
            return configured.get();
        }

        public int getInitialized() {
            return initialized.get();
        }

        public int getTerminated() {
            return terminated.get();
        }
    }

    private static class DefaultServiceA extends TestServiceBase implements ServiceA {
        public DefaultServiceA(Set<String> initRoles, Map<String, String> initProps, Map<String, List<String>> initServiceProps) {
            super(initRoles, initProps, initServiceProps);
        }
    }

    private static class DefaultServiceB extends TestServiceBase implements ServiceB {
        public DefaultServiceB(Set<String> initRoles, Map<String, String> initProps, Map<String, List<String>> initServiceProps) {
            super(initRoles, initProps, initServiceProps);
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        Hekate node = createNode(c -> {
            c.withService(() -> new DefaultServiceA(null, null, null));
            c.withService(() -> new DefaultServiceB(null, null, null));
        }).join();

        ServiceA serviceA = node.get(ServiceA.class);
        ServiceB serviceB = node.get(ServiceB.class);

        repeat(5, i -> {

            assertEquals(1, serviceA.getConfigured());
            assertEquals(i + 1, serviceA.getInitialized());
            assertEquals(i, serviceA.getTerminated());

            assertEquals(1, serviceB.getConfigured());
            assertEquals(i + 1, serviceB.getInitialized());
            assertEquals(i, serviceB.getTerminated());

            node.leave();

            node.join();
        });
    }

    @Test
    public void testConfiguring() throws Exception {
        List<HekateTestNode> nodes = new ArrayList<>();

        repeat(5, i -> {
            HekateTestNode node = createNode(c -> {
                String strIdx = String.valueOf(i + 1);

                c.withService(() -> new DefaultServiceA(toSet("a", "A"), singletonMap("a", "A"), singletonMap("_A", asList("a", strIdx))));
                c.withService(() -> new DefaultServiceB(toSet("b", "B"), singletonMap("b", "B"), singletonMap("_B", asList("b", strIdx))));
            });

            nodes.add(node);

            node.join();

            awaitForTopology(nodes);
        });

        nodes.stream()
            .map(Hekate::getLocalNode)
            .sorted(Comparator.comparingInt(ClusterNode::getJoinOrder))
            .forEach(node -> {
                assertTrue(node.hasRole("a"));
                assertTrue(node.hasRole("A"));
                assertTrue(node.hasRole("b"));
                assertTrue(node.hasRole("B"));

                assertTrue(node.hasProperty("a"));
                assertTrue(node.hasProperty("b"));

                assertEquals("A", node.getProperty("a"));
                assertEquals("B", node.getProperty("b"));

                ClusterNodeService serviceA = node.getService(ServiceA.class);
                ClusterNodeService serviceB = node.getService(ServiceB.class);

                assertNotNull(serviceA);
                assertNotNull(serviceB);

                String joinOrderStr = String.valueOf(node.getJoinOrder());

                Set<String> propA = serviceA.getProperty("_A");
                Set<String> propB = serviceB.getProperty("_B");

                say("Checking [join-order=" + joinOrderStr + ", prop-A=" + propA + ", prop-B=" + propB + ']');

                assertTrue(propA.contains("a"));
                assertTrue(propA.contains(joinOrderStr));

                assertTrue(propB.contains("b"));
                assertTrue(propB.contains(joinOrderStr));
            });
    }
}
