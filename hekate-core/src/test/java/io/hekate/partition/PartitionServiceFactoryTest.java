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

package io.hekate.partition;

import io.hekate.HekateTestBase;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PartitionServiceFactoryTest extends HekateTestBase {
    private final PartitionServiceFactory factory = new PartitionServiceFactory();

    @Test
    public void testMappers() {
        PartitionMapperConfig m1 = new PartitionMapperConfig();
        PartitionMapperConfig m2 = new PartitionMapperConfig();

        assertNull(factory.getMappers());

        factory.setMappers(Arrays.asList(m1, m2));

        assertEquals(2, factory.getMappers().size());
        assertTrue(factory.getMappers().contains(m1));
        assertTrue(factory.getMappers().contains(m2));

        factory.setMappers(null);

        assertNull(factory.getMappers());

        assertSame(factory, factory.withMapper(m1));

        assertEquals(1, factory.getMappers().size());
        assertTrue(factory.getMappers().contains(m1));

        factory.setMappers(null);

        PartitionMapperConfig mapper = factory.withMapper("test");

        assertNotNull(mapper);
        assertEquals("test", mapper.getName());
        assertTrue(factory.getMappers().contains(mapper));
    }

    @Test
    public void testProviders() {
        PartitionConfigProvider p1 = Collections::emptyList;
        PartitionConfigProvider p2 = Collections::emptyList;

        assertNull(factory.getConfigProviders());

        factory.setConfigProviders(Arrays.asList(p1, p2));

        assertEquals(2, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
        assertTrue(factory.getConfigProviders().contains(p2));

        factory.setConfigProviders(null);

        assertNull(factory.getConfigProviders());

        assertSame(factory, factory.withConfigProvider(p1));

        assertEquals(1, factory.getConfigProviders().size());
        assertTrue(factory.getConfigProviders().contains(p1));
    }
}
