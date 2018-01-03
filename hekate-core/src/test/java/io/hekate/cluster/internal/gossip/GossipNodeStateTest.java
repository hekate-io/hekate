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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeId;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GossipNodeStateTest extends HekateTestBase {
    @Test
    public void testCompare() throws Exception {
        GossipNodeState joining = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);
        GossipNodeState up = new GossipNodeState(newNode(), GossipNodeStatus.UP);
        GossipNodeState leaving = new GossipNodeState(newNode(), GossipNodeStatus.LEAVING);
        GossipNodeState down = new GossipNodeState(newNode(), GossipNodeStatus.DOWN);

        assertSame(ComparisonResult.SAME, joining.compare(joining));
        assertSame(ComparisonResult.BEFORE, joining.compare(leaving));
        assertSame(ComparisonResult.BEFORE, joining.compare(leaving));
        assertSame(ComparisonResult.BEFORE, joining.compare(down));

        assertSame(ComparisonResult.AFTER, up.compare(joining));
        assertSame(ComparisonResult.SAME, up.compare(up));
        assertSame(ComparisonResult.BEFORE, up.compare(leaving));
        assertSame(ComparisonResult.BEFORE, up.compare(down));

        assertSame(ComparisonResult.AFTER, leaving.compare(joining));
        assertSame(ComparisonResult.AFTER, leaving.compare(up));
        assertSame(ComparisonResult.SAME, leaving.compare(leaving));
        assertSame(ComparisonResult.BEFORE, leaving.compare(down));

        assertSame(ComparisonResult.AFTER, down.compare(joining));
        assertSame(ComparisonResult.AFTER, down.compare(up));
        assertSame(ComparisonResult.AFTER, down.compare(leaving));
        assertSame(ComparisonResult.SAME, down.compare(down));
    }

    @Test
    public void testCompareVersion() throws Exception {
        GossipNodeState s1 = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);
        GossipNodeState s2 = s1.suspect(newNodeId());

        assertNotSame(s2, s1);

        assertSame(ComparisonResult.BEFORE, s1.compare(s2));
        assertSame(ComparisonResult.AFTER, s2.compare(s1));

        GossipNodeState s3 = s2.unsuspected(s2.suspected());

        assertNotSame(s2, s1);

        assertSame(ComparisonResult.BEFORE, s2.compare(s3));
        assertSame(ComparisonResult.AFTER, s3.compare(s2));
    }

    @Test
    public void testCompareVersionConcurrent() throws Exception {
        GossipNodeState s1 = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);
        GossipNodeState s2 = new GossipNodeState(newNode(), GossipNodeStatus.UP);

        s1 = s1.suspect(newNodeId());

        assertSame(ComparisonResult.CONCURRENT, s1.compare(s2));
        assertSame(ComparisonResult.CONCURRENT, s2.compare(s1));

        s1 = s1.unsuspected(s1.suspected());

        assertSame(ComparisonResult.CONCURRENT, s1.compare(s2));
        assertSame(ComparisonResult.CONCURRENT, s2.compare(s1));
    }

    @Test
    public void testMerge() throws Exception {
        GossipNodeState s1 = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);
        GossipNodeState s2 = new GossipNodeState(newNode(), GossipNodeStatus.UP);

        s1 = s1.suspect(newNodeId());

        assertSame(ComparisonResult.CONCURRENT, s1.compare(s2));
        assertSame(ComparisonResult.CONCURRENT, s2.compare(s1));

        GossipNodeState s3 = s1.merge(s2);

        assertSame(ComparisonResult.BEFORE, s1.compare(s3));
        assertSame(ComparisonResult.BEFORE, s2.compare(s3));

        assertSame(ComparisonResult.AFTER, s3.compare(s1));
        assertSame(ComparisonResult.AFTER, s3.compare(s2));

        assertSame(GossipNodeStatus.UP, s3.status());
        assertEquals(s1.suspected(), s3.suspected());

        GossipNodeState s4 = s2.merge(s3);

        assertSame(ComparisonResult.BEFORE, s1.compare(s4));
        assertSame(ComparisonResult.BEFORE, s2.compare(s4));

        assertSame(ComparisonResult.AFTER, s4.compare(s1));
        assertSame(ComparisonResult.AFTER, s4.compare(s2));

        assertSame(GossipNodeStatus.UP, s4.status());
        assertEquals(s1.suspected(), s4.suspected());
    }

    @Test
    public void testUpdateState() throws Exception {
        GossipNodeState s1 = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);

        assertSame(GossipNodeStatus.JOINING, s1.status());

        GossipNodeState s2 = s1.status(GossipNodeStatus.JOINING);

        assertSame(s1, s2);
        assertSame(GossipNodeStatus.JOINING, s1.status());

        GossipNodeState s3 = s1.status(GossipNodeStatus.UP);

        assertNotSame(s1, s3);
        assertSame(GossipNodeStatus.UP, s3.status());
    }

    @Test
    public void testUpdateSuspected() throws Exception {
        GossipNodeState s = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);

        ClusterNodeId id1 = newNodeId();
        ClusterNodeId id2 = newNodeId();

        s = s.suspect(id1);

        assertTrue(s.hasSuspected());
        assertEquals(1, s.suspected().size());
        assertTrue(s.suspected().contains(id1));
        assertTrue(s.isSuspected(id1));

        s = s.status(GossipNodeStatus.UP);

        assertTrue(s.hasSuspected());
        assertEquals(1, s.suspected().size());
        assertTrue(s.suspected().contains(id1));
        assertTrue(s.isSuspected(id1));

        s = s.suspect(toSet(id1, id2));

        assertTrue(s.hasSuspected());
        assertEquals(2, s.suspected().size());
        assertTrue(s.suspected().contains(id1));
        assertTrue(s.suspected().contains(id2));
        assertTrue(s.isSuspected(id1));
        assertTrue(s.isSuspected(id2));

        s = s.unsuspected(id1);

        assertTrue(s.hasSuspected());
        assertEquals(1, s.suspected().size());
        assertTrue(s.suspected().contains(id2));
        assertTrue(s.isSuspected(id2));
        assertFalse(s.isSuspected(id1));

        s = s.unsuspected(id2);

        assertFalse(s.hasSuspected());
        assertEquals(0, s.suspected().size());
        assertFalse(s.isSuspected(id2));
        assertFalse(s.isSuspected(id1));
    }

    @Test
    public void testRemoveUnknownSuspected() throws Exception {
        GossipNodeState s1 = new GossipNodeState(newNode(), GossipNodeStatus.JOINING);

        ClusterNodeId id1 = newNodeId();
        ClusterNodeId id2 = newNodeId();

        s1 = s1.suspect(id1);

        assertTrue(s1.hasSuspected());
        assertEquals(1, s1.suspected().size());
        assertTrue(s1.suspected().contains(id1));
        assertTrue(s1.isSuspected(id1));

        assertSame(s1, s1.unsuspected(id2));
    }
}
