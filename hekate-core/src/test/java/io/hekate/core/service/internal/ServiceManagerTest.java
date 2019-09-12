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

package io.hekate.core.service.internal;

import io.hekate.HekateTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateException;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.core.service.TerminatingService;
import io.hekate.test.HekateTestError;
import io.hekate.test.HekateTestException;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ServiceManagerTest extends HekateTestBase {
    private interface ServiceA extends Service {
        // No-op.
    }

    private interface ServiceB extends Service {
        // No-op.
    }

    private static class CoreTestService implements Service {
        // No-op.
    }

    private static class NonRegisteredService implements Service {
        // No-op.
    }

    private static class TestService implements Service, InitializingService, DependentService, ConfigurableService, TerminatingService {
        private int resolved;

        private int configured;

        private int preInitialized;

        private int initialized;

        private int postInitialized;

        private int preTerminated;

        private int terminated;

        private int postTerminated;

        @Override
        public void preInitialize(InitializationContext ctx) throws HekateException {
            preInitialized++;
        }

        @Override
        public void initialize(InitializationContext ctx) throws HekateException {
            initialized++;
        }

        @Override
        public void postInitialize(InitializationContext ctx) throws HekateException {
            postInitialized++;
        }

        @Override
        public void preTerminate() throws HekateException {
            preTerminated++;
        }

        @Override
        public void terminate() throws HekateException {
            terminated++;
        }

        @Override
        public void postTerminate() throws HekateException {
            postTerminated++;
        }

        @Override
        public void resolve(DependencyContext ctx) {
            resolved++;

            assertNotNull(ctx.require(CoreTestService.class));

            assertNull(ctx.optional(NonRegisteredService.class));

            try {
                ctx.require(NonRegisteredService.class);

                fail("Error was expected.");
            } catch (HekateConfigurationException e) {
                // No-op.
            }
        }

        @Override
        public void configure(ConfigurationContext ctx) {
            configured++;
        }

        public int getInitialized() {
            return initialized;
        }

        public int getPreInitialized() {
            return preInitialized;
        }

        public int getPostInitialized() {
            return postInitialized;
        }

        public int getPreTerminated() {
            return preTerminated;
        }

        public int getTerminated() {
            return terminated;
        }

        public int getPostTerminated() {
            return postTerminated;
        }

        public int getConfigured() {
            return configured;
        }

        public int getResolved() {
            return resolved;
        }
    }

    private static class TestFactory implements ServiceFactory<TestService> {
        private final TestService service;

        private int createCalls;

        public TestFactory(TestService service) {
            this.service = service;
        }

        @Override
        public TestService createService() {
            createCalls++;

            return service;
        }

        public int getCreateCalls() {
            return createCalls;
        }
    }

    @DefaultServiceFactory(AutoCreateServiceFactory.class)
    private static class AutoCreateService extends TestService {
        // No-op.
    }

    public static class AutoCreateServiceFactory implements ServiceFactory<AutoCreateService> {
        @Override
        public AutoCreateService createService() {
            return new AutoCreateService();
        }
    }

    @Test
    public void testLifecycle() throws Exception {
        repeat(3, i -> {
            List<TestService> services = new ArrayList<>();
            List<TestFactory> factories = new ArrayList<>();

            repeat(i, j -> {
                TestService service = new TestService();

                TestFactory factory = new TestFactory(service);

                services.add(service);
                factories.add(factory);
            });

            ServiceManager m = createManager(factories);

            factories.forEach(f -> assertEquals(1, f.getCreateCalls()));

            repeat(5, j -> {
                InitializationContext ctx = mock(InitializationContext.class);
                m.preInitialize(ctx);
                m.initialize(ctx);
                m.postInitialize(ctx);

                m.preTerminate();
                m.terminate();
                m.postTerminate();

                services.forEach(s -> {
                    assertEquals(1, s.getConfigured());
                    assertEquals(1, s.getResolved());
                    assertEquals(j + 1, s.getPreInitialized());
                    assertEquals(j + 1, s.getInitialized());
                    assertEquals(j + 1, s.getPostInitialized());
                    assertEquals(j + 1, s.getPreTerminated());
                    assertEquals(j + 1, s.getTerminated());
                    assertEquals(j + 1, s.getPostTerminated());
                });
            });
        });
    }

    @Test
    public void testPreInitializationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void preInitialize(InitializationContext ctx) throws HekateException {
                super.preInitialize(ctx);

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        try {
            m.preInitialize(ctx);

            fail("Error was expected.");
        } catch (HekateException e) {
            // No-op.
        }

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(0, s1.getInitialized());
        assertEquals(1, s1.getTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(0, failed.getInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(0, s2.getPreInitialized());
        assertEquals(0, s2.getInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testInitializationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void initialize(InitializationContext ctx) throws HekateException {
                super.initialize(ctx);

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);

        try {
            m.initialize(ctx);

            fail("Error was expected.");
        } catch (HekateException e) {
            // No-op.
        }

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(1, s1.getInitialized());
        assertEquals(1, s1.getTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(1, failed.getInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(1, s2.getPreInitialized());
        assertEquals(0, s2.getInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testPostInitializationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void postInitialize(InitializationContext ctx) throws HekateException {
                super.postInitialize(ctx);

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);

        m.initialize(ctx);

        try {
            m.postInitialize(ctx);

            fail("Error was expected.");
        } catch (HekateException e) {
            // No-op.
        }

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(1, s1.getInitialized());
        assertEquals(1, s1.getPostInitialized());
        assertEquals(1, s1.getPreTerminated());
        assertEquals(1, s1.getTerminated());
        assertEquals(1, s1.getPostTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(1, failed.getInitialized());
        assertEquals(1, failed.getPostInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(1, s2.getPreInitialized());
        assertEquals(1, s2.getInitialized());
        assertEquals(0, s2.getPostInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testPreTerminationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void preTerminate() throws HekateException {
                super.preTerminate();

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);
        m.initialize(ctx);
        m.postInitialize(ctx);

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(1, s1.getInitialized());
        assertEquals(1, s1.getPostInitialized());
        assertEquals(1, s1.getPreTerminated());
        assertEquals(1, s1.getTerminated());
        assertEquals(1, s1.getPostTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(1, failed.getInitialized());
        assertEquals(1, failed.getPostInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(1, s2.getPreInitialized());
        assertEquals(1, s2.getInitialized());
        assertEquals(1, s2.getPostInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testTerminationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void terminate() throws HekateException {
                super.terminate();

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);
        m.initialize(ctx);
        m.postInitialize(ctx);

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(1, s1.getInitialized());
        assertEquals(1, s1.getPostInitialized());
        assertEquals(1, s1.getPreTerminated());
        assertEquals(1, s1.getTerminated());
        assertEquals(1, s1.getPostTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(1, failed.getInitialized());
        assertEquals(1, failed.getPostInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(1, s2.getPreInitialized());
        assertEquals(1, s2.getInitialized());
        assertEquals(1, s2.getPostInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testPostTerminationFailure() throws Exception {
        TestService failed = new TestService() {
            @Override
            public void postTerminate() throws HekateException {
                super.postTerminate();

                throw new HekateTestException(HekateTestError.MESSAGE);
            }
        };

        TestService s1 = new TestService();
        TestService s2 = new TestService();

        TestFactory f1 = new TestFactory(s1);
        TestFactory f2 = new TestFactory(s2);

        ServiceManager m = createManager(f1, new TestFactory(failed), f2);

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);
        m.initialize(ctx);
        m.postInitialize(ctx);

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, s1.getPreInitialized());
        assertEquals(1, s1.getInitialized());
        assertEquals(1, s1.getPostInitialized());
        assertEquals(1, s1.getPreTerminated());
        assertEquals(1, s1.getTerminated());
        assertEquals(1, s1.getPostTerminated());

        assertEquals(1, failed.getPreInitialized());
        assertEquals(1, failed.getInitialized());
        assertEquals(1, failed.getPostInitialized());
        assertEquals(1, failed.getPreTerminated());
        assertEquals(1, failed.getTerminated());
        assertEquals(1, failed.getPostTerminated());

        assertEquals(1, s2.getPreInitialized());
        assertEquals(1, s2.getInitialized());
        assertEquals(1, s2.getPostInitialized());
        assertEquals(1, s2.getPreTerminated());
        assertEquals(1, s2.getTerminated());
        assertEquals(1, s2.getPostTerminated());
    }

    @Test
    public void testInitTerminateOrder() throws Exception {
        List<Object> preInitOrder = new ArrayList<>();
        List<Object> initOrder = new ArrayList<>();
        List<Object> postInitOrder = new ArrayList<>();
        List<Object> preTerminateOrder = new ArrayList<>();
        List<Object> terminateOrder = new ArrayList<>();
        List<Object> posTerminateOrder = new ArrayList<>();

        class ServiceBase extends TestService {
            @Override
            public void preInitialize(InitializationContext ctx) throws HekateException {
                super.preInitialize(ctx);

                preInitOrder.add(this);
            }

            @Override
            public void initialize(InitializationContext ctx) throws HekateException {
                super.initialize(ctx);

                initOrder.add(this);
            }

            @Override
            public void postInitialize(InitializationContext ctx) throws HekateException {
                super.postInitialize(ctx);

                postInitOrder.add(this);
            }

            @Override
            public void terminate() throws HekateException {
                terminateOrder.add(this);

                super.terminate();
            }

            @Override
            public void preTerminate() throws HekateException {
                preTerminateOrder.add(this);

                super.preTerminate();
            }

            @Override
            public void postTerminate() throws HekateException {
                posTerminateOrder.add(this);

                super.postTerminate();
            }
        }

        class TestServiceA extends ServiceBase implements ServiceA {
            // No-op.
        }

        class TestServiceB extends ServiceBase implements ServiceB {
            @Override
            public void resolve(DependencyContext ctx) {
                ctx.require(TestServiceA.class);

                super.resolve(ctx);
            }
        }

        class TestServiceC extends ServiceBase implements ServiceA, ServiceB {
            @Override
            public void resolve(DependencyContext ctx) {
                ctx.require(TestServiceA.class);
                ctx.require(TestServiceB.class);

                super.resolve(ctx);
            }
        }

        TestServiceA a = new TestServiceA();
        TestServiceB b = new TestServiceB();
        TestServiceC c = new TestServiceC();

        ServiceManager m = createManager(new TestFactory(c), new TestFactory(a), new TestFactory(b));

        InitializationContext ctx = mock(InitializationContext.class);

        m.preInitialize(ctx);
        m.initialize(ctx);
        m.postInitialize(ctx);

        assertEquals(2, m.getServiceTypes().size());
        assertTrue(m.getServiceTypes().contains(ServiceA.class));
        assertTrue(m.getServiceTypes().contains(ServiceB.class));

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertSame(a, preInitOrder.get(0));
        assertSame(b, preInitOrder.get(1));
        assertSame(c, preInitOrder.get(2));

        assertSame(a, initOrder.get(0));
        assertSame(b, initOrder.get(1));
        assertSame(c, initOrder.get(2));

        assertSame(a, postInitOrder.get(0));
        assertSame(b, postInitOrder.get(1));
        assertSame(c, postInitOrder.get(2));

        assertSame(a, preTerminateOrder.get(2));
        assertSame(b, preTerminateOrder.get(1));
        assertSame(c, preTerminateOrder.get(0));

        assertSame(a, terminateOrder.get(2));
        assertSame(b, terminateOrder.get(1));
        assertSame(c, terminateOrder.get(0));

        assertSame(a, posTerminateOrder.get(2));
        assertSame(b, posTerminateOrder.get(1));
        assertSame(c, posTerminateOrder.get(0));
    }

    @Test
    public void testAutoCreate() throws Exception {
        class ServiceA extends TestService {
            private AutoCreateService auto;

            private boolean notFoundError;

            @Override
            public void resolve(DependencyContext ctx) {
                auto = ctx.require(AutoCreateService.class);

                try {
                    ctx.require(NonRegisteredService.class);
                } catch (HekateConfigurationException e) {
                    notFoundError = true;
                }
            }
        }

        class ServiceB extends TestService {
            private AutoCreateService auto;

            @Override
            public void resolve(DependencyContext ctx) {
                auto = ctx.require(AutoCreateService.class);

                ctx.require(ServiceA.class);
            }
        }

        ServiceManager m = createManager(new TestFactory(new ServiceA()), new TestFactory(new ServiceB()));

        InitializationContext ctx = mock(InitializationContext.class);

        m.initialize(ctx);

        assertNotNull(m.findService(ServiceA.class).auto);
        assertTrue(m.findService(ServiceA.class).notFoundError);

        assertNotNull(m.findService(ServiceB.class).auto);

        AutoCreateService auto = m.findService(AutoCreateService.class);

        assertNotNull(auto);

        m.preTerminate();
        m.terminate();
        m.postTerminate();

        assertEquals(1, auto.getConfigured());
        assertEquals(1, auto.getInitialized());
        assertEquals(1, auto.getPreTerminated());
        assertEquals(1, auto.getTerminated());
        assertEquals(1, auto.getPostTerminated());
    }

    private ServiceManager createManager(TestFactory... factories) throws HekateException {
        return createManager(Arrays.asList(factories));
    }

    private ServiceManager createManager(List<TestFactory> factories) throws HekateException {
        List<CoreTestService> services = Collections.singletonList(new CoreTestService());

        ServiceManager manager = new ServiceManager(
            "test-node",
            "test-cluster",
            mock(Hekate.class),
            mock(MeterRegistry.class),
            services,
            emptyList(),
            factories
        );

        manager.instantiate();

        return manager;
    }
}
