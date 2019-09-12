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

package io.hekate.lock;

import io.hekate.HekateTestBase;
import java.util.ArrayList;
import java.util.Collections;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class LockServiceFactoryTest extends HekateTestBase {
    private final LockServiceFactory factory = new LockServiceFactory();

    @Test
    public void testRegions() {
        assertNull(factory.getRegions());

        factory.setRegions(new ArrayList<>(asList(new LockRegionConfig().withName("test1"), new LockRegionConfig().withName("test2"))));

        assertEquals(2, factory.getRegions().size());
        assertTrue(factory.getRegions().stream().anyMatch(c -> "test1".equals(c.getName())));
        assertTrue(factory.getRegions().stream().anyMatch(c -> "test2".equals(c.getName())));

        assertSame(factory, factory.withRegion(new LockRegionConfig().withName("test3")));

        assertEquals(3, factory.getRegions().size());
        assertTrue(factory.getRegions().stream().anyMatch(c -> "test1".equals(c.getName())));
        assertTrue(factory.getRegions().stream().anyMatch(c -> "test2".equals(c.getName())));
        assertTrue(factory.getRegions().stream().anyMatch(c -> "test3".equals(c.getName())));

        factory.setRegions(null);

        assertNull(factory.getRegions());

        assertSame(factory, factory.withRegion(new LockRegionConfig().withName("test4")));

        assertEquals(1, factory.getRegions().size());
        assertSame(factory, factory.withRegion(new LockRegionConfig().withName("test4")));
        assertTrue(factory.getRegions().stream().allMatch(c -> "test4".equals(c.getName())));
    }

    @Test
    public void testRegionProviders() {
        LockConfigProvider p1 = Collections::emptyList;
        LockConfigProvider p2 = Collections::emptyList;

        assertNull(factory.getConfigProviders());

        factory.setConfigProviders(asList(p1, p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));

        factory.setConfigProviders(null);

        assertNull(factory.getConfigProviders());

        assertSame(factory, factory.withConfigProvider(p1));

        assertEquals(1, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
    }

    @Test
    public void testRetryInterval() {
        assertEquals(LockServiceFactory.DEFAULT_RETRY_INTERVAL, factory.getRetryInterval());

        factory.setRetryInterval(10000);

        assertEquals(10000, factory.getRetryInterval());

        assertSame(factory, factory.withRetryInterval(1000));
        assertEquals(1000, factory.getRetryInterval());
    }

    @Test
    public void testWorkerThreads() {
        assertEquals(LockServiceFactory.DEFAULT_WORKER_THREADS, factory.getWorkerThreads());

        factory.setWorkerThreads(LockServiceFactory.DEFAULT_WORKER_THREADS + 10000);

        assertEquals(LockServiceFactory.DEFAULT_WORKER_THREADS + 10000, factory.getWorkerThreads());

        assertSame(factory, factory.withWorkerThreads(1000));
        assertEquals(1000, factory.getWorkerThreads());
    }

    @Test
    public void testNioThreads() {
        assertEquals(0, factory.getNioThreads());

        factory.setNioThreads(1000);

        assertEquals(1000, factory.getNioThreads());

        assertSame(factory, factory.withNioThreads(100));
        assertEquals(100, factory.getNioThreads());
    }

    @Test
    public void testCreateService() {
        assertNotNull(factory.createService());
    }
}
