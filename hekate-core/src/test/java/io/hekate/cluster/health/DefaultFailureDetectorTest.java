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

package io.hekate.cluster.health;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterAddress;
import java.util.Collection;
import java.util.HashSet;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class DefaultFailureDetectorTest extends HekateTestBase {
    private DefaultFailureDetector mgr;

    private ClusterAddress n1;

    private ClusterAddress n2;

    private ClusterAddress n3;

    private ClusterAddress n4;

    @Before
    public void setUp() throws Exception {
        n1 = newNode(1).address();
        n2 = newNode(2).address();
        n3 = newNode(3).address();
        n4 = newNode(4).address();

        DefaultFailureDetectorConfig cfg = new DefaultFailureDetectorConfig();

        cfg.setHeartbeatInterval(1);
        cfg.setHeartbeatLossThreshold(1);

        mgr = new DefaultFailureDetector(cfg);

        mgr.initialize(() -> n1);

        mgr.update(toSet(n1, n2, n3, n4));
    }

    @Test
    public void testIsAliveWithAllAlive() throws Exception {
        Collection<ClusterAddress> heartbeats = mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));

        heartbeats.forEach(mgr::onHeartbeatReply);

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));
    }

    @Test
    public void testIsAliveWithFailed() throws Exception {
        mgr.heartbeatTick();

        mgr.onHeartbeatReply(n1);
        mgr.onHeartbeatReply(n3);
        mgr.onHeartbeatReply(n4);

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertFalse(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertFalse(mgr.isAlive(n2));
        assertFalse(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));
    }

    @Test
    public void testAliveAfterResume() throws Exception {
        mgr.heartbeatTick();

        mgr.onHeartbeatReply(n1);
        mgr.onHeartbeatReply(n3);
        mgr.onHeartbeatReply(n4);

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));

        mgr.heartbeatTick();

        assertTrue(mgr.isAlive(n1));
        assertFalse(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));

        mgr.onHeartbeatReply(n2);

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));
        assertTrue(mgr.isAlive(newNode(100).address()));
    }

    @Test
    public void testIsMonitored() throws Exception {
        assertFalse(mgr.isMonitored(n1));

        assertTrue(mgr.isMonitored(n2));
        assertTrue(mgr.isMonitored(n3));

        assertFalse(mgr.isMonitored(n4));
        assertFalse(mgr.isMonitored(newNode(100).address()));
    }

    @Test
    public void testUpdate() throws Exception {
        mgr.update(toSet(n1, n3, n4));

        assertFalse(mgr.isMonitored(n1));
        assertFalse(mgr.isMonitored(n2));
        assertTrue(mgr.isMonitored(n3));
        assertTrue(mgr.isMonitored(n4));
        assertFalse(mgr.isMonitored(newNode(100).address()));

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));

        mgr.update(toSet(n1, n2, n3, n4));

        assertFalse(mgr.isMonitored(n1));
        assertTrue(mgr.isMonitored(n2));
        assertTrue(mgr.isMonitored(n3));
        assertFalse(mgr.isMonitored(n4));
        assertFalse(mgr.isMonitored(newNode(100).address()));

        assertTrue(mgr.isAlive(n1));
        assertTrue(mgr.isAlive(n2));
        assertTrue(mgr.isAlive(n3));
        assertTrue(mgr.isAlive(n4));

    }

    @Test
    public void testUpdateWithFailed() throws Exception {
        mgr.heartbeatTick();

        mgr.onHeartbeatReply(n1);
        mgr.onHeartbeatReply(n3);
        mgr.onHeartbeatReply(n4);

        mgr.heartbeatTick();

        mgr.update(toSet(n1, n3, n4));

        assertFalse(mgr.isMonitored(n1));
        assertFalse(mgr.isMonitored(n2));
        assertTrue(mgr.isMonitored(n3));
        assertTrue(mgr.isMonitored(n4));
        assertFalse(mgr.isMonitored(newNode(100).address()));
    }

    @Test
    public void testGetHeartbeats() throws Exception {
        Collection<ClusterAddress> heartbeats = mgr.heartbeatTick();

        assertEquals(2, heartbeats.size());
        assertEquals(toSet(n2, n3), new HashSet<>(heartbeats));

        heartbeats = mgr.heartbeatTick();

        assertEquals(2, heartbeats.size());
        assertEquals(toSet(n2, n3), new HashSet<>(heartbeats));

        heartbeats = mgr.heartbeatTick();

        assertEquals(2, heartbeats.size());
        assertEquals(toSet(n2, n3), new HashSet<>(heartbeats));

        heartbeats = mgr.heartbeatTick();

        assertEquals(3, heartbeats.size());
        assertEquals(toSet(n2, n3, n4), new HashSet<>(heartbeats));

        mgr.update(toSet(n1, n4));

        heartbeats = mgr.heartbeatTick();

        assertEquals(1, heartbeats.size());
        assertEquals(toSet(n4), new HashSet<>(heartbeats));
    }

    @Test
    public void testGetHeartbeatsSingleNode() throws Exception {
        mgr.update(toSet(n1));

        assertTrue(mgr.heartbeatTick().isEmpty());
    }

    @Test
    public void testMonitorOrder() throws Exception {
        assertTrue(mgr.isMonitored(n2));
        assertTrue(mgr.isMonitored(n3));

        ClusterAddress before1 = newNode(-2).address();
        ClusterAddress before2 = newNode(-1).address();

        mgr.update(toSet(n1, before1, before2));

        assertFalse(mgr.isMonitored(n2));
        assertFalse(mgr.isMonitored(n3));
        assertTrue(mgr.isMonitored(before1));
        assertTrue(mgr.isMonitored(before2));

        mgr.update(toSet(n1, before2));

        assertFalse(mgr.isMonitored(n2));
        assertFalse(mgr.isMonitored(before1));
        assertTrue(mgr.isMonitored(before2));
    }
}
