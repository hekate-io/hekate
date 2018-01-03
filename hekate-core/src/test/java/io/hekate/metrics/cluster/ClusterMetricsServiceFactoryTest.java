/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.metrics.cluster;

import io.hekate.HekateTestBase;
import io.hekate.metrics.MetricFilter;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ClusterMetricsServiceFactoryTest extends HekateTestBase {
    private final ClusterMetricsServiceFactory factory = new ClusterMetricsServiceFactory();

    @Test
    public void testEnabled() {
        assertFalse(factory.isEnabled());

        factory.setEnabled(true);

        assertTrue(factory.isEnabled());

        assertSame(factory, factory.withEnabled(false));

        assertFalse(factory.isEnabled());
    }

    @Test
    public void testReplicationInterval() {
        assertEquals(ClusterMetricsServiceFactory.DEFAULT_REPLICATION_INTERVAL, factory.getReplicationInterval());

        factory.setReplicationInterval(100000);

        assertEquals(100000, factory.getReplicationInterval());

        assertSame(factory, factory.withReplicationInterval(2000));

        assertEquals(2000, factory.getReplicationInterval());
    }

    @Test
    public void testReplicationFilter() {
        MetricFilter f = mock(MetricFilter.class);

        assertNull(factory.getReplicationFilter());

        factory.setReplicationFilter(f);

        assertSame(f, factory.getReplicationFilter());

        factory.setReplicationFilter(null);

        assertNull(factory.getReplicationFilter());

        assertSame(factory, factory.withReplicationFilter(f));

        assertSame(f, factory.getReplicationFilter());
    }
}
