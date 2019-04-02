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

package io.hekate.core.service;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.HekateTestNode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

        private final Map<String, Object> initProps;

        public TestServiceBase(Map<String, Object> initProps) {
            this.initProps = initProps;
        }

        @Override
        public void configure(ConfigurationContext ctx) {
            configured.incrementAndGet();

            if (initProps != null) {
                initProps.forEach((k, v) -> {
                    if (v instanceof String) {
                        ctx.setStringProperty(k, (String)v);
                    } else if (v instanceof Integer) {
                        ctx.setIntProperty(k, (Integer)v);
                    } else if (v instanceof Long) {
                        ctx.setLongProperty(k, (Long)v);
                    } else if (v instanceof Boolean) {
                        ctx.setBoolProperty(k, (Boolean)v);
                    } else {
                        throw new IllegalArgumentException("Unsupported property type: " + v.getClass());
                    }
                });
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
        public DefaultServiceA(Map<String, Object> initServiceProps) {
            super(initServiceProps);
        }
    }

    private static class DefaultServiceB extends TestServiceBase implements ServiceB {
        public DefaultServiceB(Map<String, Object> initServiceProps) {
            super(initServiceProps);
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        Hekate node = createNode(c -> {
            c.withService(() -> new DefaultServiceA(null));
            c.withService(() -> new DefaultServiceB(null));
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
                String strVal = String.valueOf(i + 1);
                Integer intVal = i + 10000;
                Long longVal = i + 1000000L;
                Boolean boolVal = i % 2 == 0;

                Map<String, Object> propsA = new HashMap<>();

                propsA.put("A-string", strVal);
                propsA.put("A-int", intVal);
                propsA.put("A-long", longVal);
                propsA.put("A-bool", boolVal);

                Map<String, Object> propsB = new HashMap<>();

                propsB.put("B-string", strVal);
                propsB.put("B-int", intVal);
                propsB.put("B-long", longVal);
                propsB.put("B-bool", boolVal);

                c.withService(() -> new DefaultServiceA(propsA));
                c.withService(() -> new DefaultServiceB(propsB));
            });

            nodes.add(node);

            node.join();

            awaitForTopology(nodes);
        });

        nodes.stream()
            .map(Hekate::localNode)
            .sorted(Comparator.comparingInt(ClusterNode::joinOrder))
            .forEach(node -> {
                ServiceInfo serviceA = node.service(ServiceA.class);
                ServiceInfo serviceB = node.service(ServiceB.class);

                assertNotNull(serviceA);
                assertNotNull(serviceB);

                int seed = node.joinOrder() - 1;

                String strVal = String.valueOf(seed + 1);
                Integer intVal = seed + 10000;
                Long longVal = seed + 1000000L;
                Boolean boolVal = seed % 2 == 0;

                assertEquals(strVal, serviceA.stringProperty("A-string"));
                assertEquals(intVal, serviceA.intProperty("A-int"));
                assertEquals(longVal, serviceA.longProperty("A-long"));
                assertEquals(boolVal, serviceA.boolProperty("A-bool"));

                assertEquals(strVal, serviceB.stringProperty("B-string"));
                assertEquals(intVal, serviceB.intProperty("B-int"));
                assertEquals(longVal, serviceB.longProperty("B-long"));
                assertEquals(boolVal, serviceB.boolProperty("B-bool"));
            });
    }
}
