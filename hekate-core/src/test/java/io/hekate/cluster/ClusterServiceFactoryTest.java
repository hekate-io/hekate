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

package io.hekate.cluster;

import io.hekate.HekateTestBase;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.health.FailureDetectorMock;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.HekateConfigurationException;
import io.hekate.test.SeedNodeProviderMock;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterServiceFactoryTest extends HekateTestBase {
    private final ClusterServiceFactory factory = new ClusterServiceFactory();

    @Test
    public void testSplitBrainAction() {
        assertSame(SplitBrainAction.TERMINATE, factory.getSplitBrainAction());

        factory.setSplitBrainAction(SplitBrainAction.REJOIN);

        assertSame(SplitBrainAction.REJOIN, factory.getSplitBrainAction());

        assertSame(factory, factory.withSplitBrainAction(SplitBrainAction.TERMINATE));

        assertSame(SplitBrainAction.TERMINATE, factory.getSplitBrainAction());

        try {
            factory.setSplitBrainAction(null);

            fail("Error was expected.");
        } catch (HekateConfigurationException e) {
            assertEquals(e.toString(), e.getMessage(), ClusterServiceFactory.class.getSimpleName()
                + ": split-brain action must be not null.");
        }

        assertSame(SplitBrainAction.TERMINATE, factory.getSplitBrainAction());
    }

    @Test
    public void testSplitBrainDetector() {
        SplitBrainDetector d1 = localNode -> true;
        SplitBrainDetector d2 = localNode -> true;

        assertNull(factory.getSplitBrainDetector());

        factory.setSplitBrainDetector(d1);

        assertSame(d1, factory.getSplitBrainDetector());

        assertSame(factory, factory.withSplitBrainDetector(d2));

        assertSame(d2, factory.getSplitBrainDetector());

        factory.setSplitBrainDetector(null);

        assertNull(factory.getSplitBrainDetector());
    }

    @Test
    public void testGossipInterval() {
        assertEquals(ClusterServiceFactory.DEFAULT_GOSSIP_INTERVAL, factory.getGossipInterval());

        factory.setGossipInterval(10000);

        assertEquals(10000, factory.getGossipInterval());

        assertEquals(10001, factory.withGossipInterval(10001).getGossipInterval());
    }

    @Test
    public void testSpeedUpGossipSize() {
        assertEquals(ClusterServiceFactory.DEFAULT_SPEED_UP_SIZE, factory.getSpeedUpGossipSize());

        factory.setSpeedUpGossipSize(10000);

        assertEquals(10000, factory.getSpeedUpGossipSize());

        assertEquals(10001, factory.withSpeedUpGossipSize(10001).getSpeedUpGossipSize());
    }

    @Test
    public void testSeedNodeProvider() {
        assertNull(factory.getSeedNodeProvider());

        SeedNodeProvider provider1 = new SeedNodeProviderMock();

        factory.setSeedNodeProvider(provider1);

        assertSame(provider1, factory.getSeedNodeProvider());

        SeedNodeProvider provider2 = new SeedNodeProviderMock();

        assertSame(provider2, factory.withSeedNodeProvider(provider2).getSeedNodeProvider());
    }

    @Test
    public void testFailureDetector() throws Exception {
        assertNotNull(factory.getFailureDetector());

        FailureDetector detector1 = new FailureDetectorMock(newNode(), new FailureDetectorMock.GlobalNodesState());

        factory.setFailureDetector(detector1);

        assertSame(detector1, factory.getFailureDetector());

        FailureDetector detector2 = new FailureDetectorMock(newNode(), new FailureDetectorMock.GlobalNodesState());

        assertSame(detector2, factory.withFailureDetector(detector2).getFailureDetector());
    }

    @Test
    public void testClusterListeners() {
        assertNull(factory.getClusterListeners());

        ClusterEventListener listener = event -> {
            // No-op.
        };

        factory.setClusterListeners(Collections.singletonList(listener));

        assertNotNull(factory.getClusterListeners());
        assertTrue(factory.getClusterListeners().contains(listener));

        factory.setClusterListeners(null);

        assertNull(factory.getClusterListeners());

        assertTrue(factory.withClusterListener(listener).getClusterListeners().contains(listener));

        factory.setClusterListeners(null);

        assertNull(factory.getClusterListeners());
    }

    @Test
    public void testAcceptors() {
        assertNull(factory.getAcceptors());

        ClusterAcceptor acceptor = (newNode, node) -> null;

        factory.setAcceptors(Collections.singletonList(acceptor));

        assertNotNull(factory.getAcceptors());
        assertTrue(factory.getAcceptors().contains(acceptor));

        factory.setAcceptors(null);

        assertNull(factory.getAcceptors());

        assertTrue(factory.withAcceptor(acceptor).getAcceptors().contains(acceptor));

        factory.setAcceptors(null);

        assertNull(factory.getAcceptors());
    }
}
