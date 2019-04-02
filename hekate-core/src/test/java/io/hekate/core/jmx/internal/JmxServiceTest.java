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

package io.hekate.core.jmx.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateUncheckedException;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceException;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.Optional;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class JmxServiceTest extends HekateNodeTestBase {
    @MXBean
    public interface TestMxBeanA {
        String getTestValue();
    }

    @MXBean
    public interface TestMxBeanB {
        String getTestValue();
    }

    @MXBean
    public interface NonCompliantMxBean {
        InetSocketAddress getAddress();
    }

    public static class InvalidMxBean implements TestMxBeanA, TestMxBeanB {
        @Override
        public String getTestValue() {
            return null;
        }
    }

    private HekateTestNode node;

    private JmxService jmx;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createNode(boot -> boot.withService(JmxServiceFactory.class)).join();

        jmx = node.get(JmxService.class);
    }

    @Test
    public void testNameFor() throws Exception {
        String domain = JmxServiceFactory.DEFAULT_DOMAIN;

        ObjectName name1 = new ObjectName(domain, "type", TestMxBeanA.class.getSimpleName());

        Hashtable<String, String> attrs = new Hashtable<>();

        attrs.put("type", TestMxBeanB.class.getSimpleName());
        attrs.put("name", "test-name");

        ObjectName name2 = new ObjectName(domain, attrs);

        assertEquals(name1, jmx.nameFor(TestMxBeanA.class));
        assertEquals(name2, jmx.nameFor(TestMxBeanB.class, "test-name"));

        HekateUncheckedException err = expect(HekateUncheckedException.class, () -> jmx.nameFor(TestMxBeanA.class, ","));

        assertTrue(ErrorUtils.stackTrace(err), err.isCausedBy(MalformedObjectNameException.class));
    }

    @Test
    public void testRegisterInvalidMxBean() throws Exception {
        // Not annotated with @MXBean (should not throw an error).
        assertFalse(jmx.register(new Object()).isPresent());

        // More than one @MXBean-annotated interface.
        JmxServiceException err1 = expect(JmxServiceException.class, () -> jmx.register(new InvalidMxBean()));

        assertTrue(err1.getMessage(), err1.getMessage().startsWith("JMX object implements more than one @MXBean-annotated interfaces ["));

        // Non-compliant MXBean.
        JmxServiceException err2 = expect(JmxServiceException.class, () -> jmx.register(mock(NonCompliantMxBean.class)));

        assertEquals(ErrorUtils.stackTrace(err2), NotCompliantMBeanException.class, err2.getCause().getClass());

        // Duplicated object name.
        ObjectName dupName = jmx.nameFor(TestMxBeanA.class);

        try {
            jmx.server().registerMBean(mock(TestMxBeanA.class), dupName);

            JmxServiceException err3 = expect(JmxServiceException.class, () -> jmx.register(mock(TestMxBeanA.class)));

            assertEquals(ErrorUtils.stackTrace(err3), InstanceAlreadyExistsException.class, err3.getCause().getClass());
        } finally {
            jmx.server().unregisterMBean(dupName);
        }
    }

    @Test
    public void test() throws Exception {
        TestMxBeanA bean1 = mock(TestMxBeanA.class);
        TestMxBeanA bean2 = mock(TestMxBeanA.class);
        TestMxBeanB bean3 = mock(TestMxBeanB.class);

        repeat(3, i -> {
            when(bean1.getTestValue()).thenReturn("test-1");
            when(bean2.getTestValue()).thenReturn("test-2");
            when(bean3.getTestValue()).thenReturn("test-3");

            if (node.state() == Hekate.State.DOWN) {
                node.join();
            }

            JmxService jmx = node.get(JmxService.class);

            ObjectName name1 = jmx.register(bean1, "test-name-1").get();
            ObjectName name2 = jmx.register(bean2, "test-name-2").get();
            ObjectName name3 = jmx.register(bean3).get();

            assertTrue(jmx.names().contains(name1));
            assertTrue(jmx.names().contains(name2));
            assertTrue(jmx.names().contains(name3));

            assertEquals("test-1", jmxAttribute(name1, "TestValue", String.class, node));
            assertEquals("test-2", jmxAttribute(name2, "TestValue", String.class, node));
            assertEquals("test-3", jmxAttribute(name3, "TestValue", String.class, node));

            verify(bean1).getTestValue();
            verify(bean2).getTestValue();
            verify(bean3).getTestValue();

            verifyNoMoreInteractions(bean1, bean2, bean3);

            node.leave();

            assertFalse(jmx.server().isRegistered(name1));
            assertFalse(jmx.server().isRegistered(name2));
            assertFalse(jmx.server().isRegistered(name3));

            reset(bean1, bean2, bean3);
        });
    }

    @Test
    public void testCollection() throws Exception {
        TestMxBeanA bean1 = mock(TestMxBeanA.class);
        TestMxBeanB bean2 = mock(TestMxBeanB.class);

        repeat(3, i -> {
            when(bean1.getTestValue()).thenReturn("test-1");
            when(bean2.getTestValue()).thenReturn("test-2");

            if (node.state() == Hekate.State.DOWN) {
                node.join();
            }

            JmxService jmx = node.get(JmxService.class);

            List<Object> beans = Arrays.asList(
                bean1,
                bean2,
                null // Null should be filtered out during registration.
            );

            Optional<ObjectName> noName = jmx.register(beans);

            assertFalse(noName.isPresent());

            ObjectName name1 = jmx.nameFor(TestMxBeanA.class);
            ObjectName name2 = jmx.nameFor(TestMxBeanB.class);

            assertTrue(jmx.names().contains(name1));
            assertTrue(jmx.names().contains(name2));

            assertEquals("test-1", jmxAttribute(name1, "TestValue", String.class, node));
            assertEquals("test-2", jmxAttribute(name2, "TestValue", String.class, node));

            verify(bean1).getTestValue();
            verify(bean2).getTestValue();

            verifyNoMoreInteractions(bean1, bean2);

            node.leave();

            assertFalse(jmx.server().isRegistered(name1));
            assertFalse(jmx.server().isRegistered(name2));

            reset(bean1, bean2);
        });
    }

    @Test
    public void testToString() {
        JmxService service = new JmxServiceFactory().createService();

        assertEquals(ToString.format(JmxService.class, service), service.toString());
    }
}
