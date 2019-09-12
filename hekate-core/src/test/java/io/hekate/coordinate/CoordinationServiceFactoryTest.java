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

package io.hekate.coordinate;

import io.hekate.HekateTestBase;
import java.util.Arrays;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class CoordinationServiceFactoryTest extends HekateTestBase {
    private final CoordinationServiceFactory factory = new CoordinationServiceFactory();

    @Test
    public void testProcesses() throws Exception {
        CoordinationProcessConfig cfg1 = new CoordinationProcessConfig();
        CoordinationProcessConfig cfg2 = new CoordinationProcessConfig();

        assertNull(factory.getProcesses());

        factory.setProcesses(Arrays.asList(cfg1, cfg2));

        assertEquals(2, factory.getProcesses().size());
        assertTrue(factory.getProcesses().contains(cfg1));
        assertTrue(factory.getProcesses().contains(cfg2));

        factory.setProcesses(null);

        assertSame(factory, factory.withProcess(cfg1));
        assertSame(factory, factory.withProcess(cfg2));

        assertEquals(2, factory.getProcesses().size());
        assertTrue(factory.getProcesses().contains(cfg1));
        assertTrue(factory.getProcesses().contains(cfg2));

        factory.setProcesses(null);

        CoordinationProcessConfig process = factory.withProcess("test");

        assertNotNull(process);
        assertEquals("test", process.getName());
        assertTrue(factory.getProcesses().contains(process));
    }

    @Test
    public void testProviders() throws Exception {
        CoordinationConfigProvider p1 = () -> null;
        CoordinationConfigProvider p2 = () -> null;

        assertNull(factory.getConfigProviders());

        factory.setConfigProviders(Arrays.asList(p1, p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));

        factory.setConfigProviders(null);

        assertSame(factory, factory.withConfigProvider(p1));
        assertSame(factory, factory.withConfigProvider(p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));
    }

    @Test
    public void testNioThreads() throws Exception {
        assertEquals(0, factory.getNioThreads());

        factory.setNioThreads(10000);

        assertEquals(10000, factory.getNioThreads());

        assertSame(factory, factory.withNioThreads(1000));
        assertEquals(1000, factory.getNioThreads());
    }

    @Test
    public void testRetryInterval() throws Exception {
        assertEquals(CoordinationServiceFactory.DEFAULT_RETRY_INTERVAL, factory.getRetryInterval());

        factory.setRetryInterval(10000);

        assertEquals(10000, factory.getRetryInterval());

        assertSame(factory, factory.withRetryInterval(1000));
        assertEquals(1000, factory.getRetryInterval());
    }

    @Test
    public void testIdleSocketTimeout() throws Exception {
        assertEquals(CoordinationServiceFactory.DEFAULT_IDLE_SOCKET_TIMEOUT, factory.getIdleSocketTimeout());

        factory.setIdleSocketTimeout(10000);

        assertEquals(10000, factory.getIdleSocketTimeout());

        assertSame(factory, factory.withIdleSocketTimeout(1000));
        assertEquals(1000, factory.getIdleSocketTimeout());
    }

    @Test
    public void testCreateService() {
        assertNotNull(factory.createService());
    }
}
