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

package io.hekate.cluster.internal.gossip;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class GossipTest extends HekateTestBase {
    @Test
    public void testEmpty() {
        Gossip gossip = new Gossip();

        assertEquals(0, gossip.getVersion());
        assertEquals(0, gossip.getMaxJoinOrder());
        assertTrue(gossip.getAllSuspected().isEmpty());
        assertTrue(gossip.getMembers().isEmpty());
        assertTrue(gossip.getSeen().isEmpty());
        assertTrue(gossip.getAllSuspected().isEmpty());
        assertFalse(gossip.hasMembers());
        assertNotNull(gossip.stream());

        SuspectedNodesView suspectedView = gossip.getSuspectedView();

        assertNotNull(suspectedView);
        assertTrue(suspectedView.isEmpty());
        assertTrue(suspectedView.getSuspected().isEmpty());
    }

    @Test
    public void testNonEmpty() throws Exception {
        Gossip g1 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g2 = g1.update(s1.getId(), s1);

        assertNotSame(g1, g2);
        assertTrue(g2.hasMembers());
        assertEquals(1, g2.getMembers().size());
        assertEquals(1, g2.getSeen().size());
        assertTrue(g2.hasSeen(s1.getId()));
        assertTrue(g2.hasMember(s1.getId()));
        assertEquals(s1, g2.getMember(s1.getId()));

        Gossip g3 = g2.update(s1.getId(), s2);

        assertNotSame(g2, g3);
        assertTrue(g3.hasMembers());
        assertEquals(2, g3.getMembers().size());
        assertEquals(1, g3.getSeen().size());
        assertTrue(g3.hasSeen(s1.getId()));
        assertFalse(g3.hasSeen(s2.getId()));
        assertTrue(g3.hasMember(s1.getId()));
        assertTrue(g3.hasMember(s2.getId()));
        assertEquals(s1, g3.getMember(s1.getId()));
        assertEquals(s2, g3.getMember(s2.getId()));

        Gossip g4 = g3.update(s1.getId(), s3);

        assertNotSame(g3, g4);
        assertTrue(g4.hasMembers());
        assertEquals(3, g4.getMembers().size());
        assertEquals(1, g4.getSeen().size());
        assertTrue(g4.hasSeen(s1.getId()));
        assertFalse(g4.hasSeen(s2.getId()));
        assertFalse(g4.hasSeen(s3.getId()));
        assertTrue(g4.hasMember(s1.getId()));
        assertTrue(g4.hasMember(s2.getId()));
        assertTrue(g4.hasMember(s3.getId()));
        assertEquals(s1, g4.getMember(s1.getId()));
        assertEquals(s2, g4.getMember(s2.getId()));
        assertEquals(s3, g4.getMember(s3.getId()));

        Gossip g5 = g4.seen(s2.getId()).seen(s3.getId());

        assertNotSame(g4, g5);
        assertTrue(g5.hasMembers());
        assertEquals(3, g5.getMembers().size());
        assertEquals(3, g5.getSeen().size());
        assertTrue(g5.hasSeen(s1.getId()));
        assertTrue(g5.hasSeen(s2.getId()));
        assertTrue(g5.hasSeen(s3.getId()));
        assertTrue(g5.hasMember(s1.getId()));
        assertTrue(g5.hasMember(s2.getId()));
        assertTrue(g5.hasMember(s3.getId()));
        assertEquals(s1, g5.getMember(s1.getId()));
        assertEquals(s2, g5.getMember(s2.getId()));
        assertEquals(s3, g5.getMember(s3.getId()));

        Gossip g6 = g5.purge(s1.getId(), Collections.singleton(s3.getId()));

        assertNotSame(g5, g6);
        assertTrue(g6.hasMembers());
        assertEquals(2, g6.getMembers().size());
        assertEquals(1, g6.getSeen().size());
        assertTrue(g6.hasSeen(s1.getId()));
        assertFalse(g6.hasSeen(s2.getId()));
        assertFalse(g6.hasSeen(s3.getId()));
        assertTrue(g6.hasMember(s1.getId()));
        assertTrue(g6.hasMember(s2.getId()));
        assertFalse(g6.hasMember(s3.getId()));
        assertEquals(s1, g6.getMember(s1.getId()));
        assertEquals(s2, g6.getMember(s2.getId()));
        assertNull(g6.getMember(s3.getId()));

        Gossip g7 = g6.maxJoinOrder(1000);

        assertNotSame(g5, g6);
        assertEquals(1000, g7.getMaxJoinOrder());
    }

    @Test
    public void testSeen() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g1 = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertEquals(1, g1.getSeen().size());
        assertTrue(g1.hasSeen(s1.getId()));
        assertFalse(g1.hasSeen(s2.getId()));
        assertFalse(g1.hasSeen(s3.getId()));

        assertSame(g1, g1.seen(s1.getId()));

        Gossip g2 = g1.seen(s2.getId());

        assertNotSame(g1, g2);
        assertEquals(2, g2.getSeen().size());
        assertTrue(g2.hasSeen(s1.getId()));
        assertTrue(g2.hasSeen(s2.getId()));
        assertFalse(g1.hasSeen(s3.getId()));

        Gossip g3 = g2.seen(toSet(s1.getId(), s2.getId(), s3.getId()));

        assertNotSame(g2, g3);
        assertEquals(3, g3.getSeen().size());
        assertTrue(g3.hasSeen(s1.getId()));
        assertTrue(g3.hasSeen(s2.getId()));
        assertTrue(g3.hasSeen(s3.getId()));

        assertTrue(g3.hasSeen(toSet(s1.getId(), s2.getId(), s3.getId())));
        assertFalse(g3.hasSeen(toSet(s1.getId(), s2.getId(), s3.getId(), newNodeId())));

        assertSame(g3, g3.seen(toSet(s1.getId(), s2.getId(), s3.getId())));
    }

    @Test
    public void testSeenInherit() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);
        GossipNodeState s4 = newState(GossipNodeStatus.UP);
        GossipNodeState s5Down = newState(GossipNodeStatus.DOWN);

        Gossip g1 = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        Gossip g2 = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3)
            .update(s1.getId(), s4)
            .update(s1.getId(), s5Down)
            .seen(s2.getId())
            .seen(s3.getId())
            .seen(s4.getId())
            .seen(s5Down.getId());

        g1 = g1.inheritSeen(s1.getId(), g2);

        assertEquals(1, g1.getSeen().size());
        assertTrue(g1.hasSeen(s1.getId()));
        assertFalse(g1.hasSeen(s5Down.getId()));

        g1 = g1.update(s1.getId(), s5Down);

        assertEquals(1, g1.getSeen().size());
        assertTrue(g1.hasSeen(s1.getId()));
        assertFalse(g1.hasSeen(s5Down.getId()));

        g1 = g1.inheritSeen(s1.getId(), g2);

        assertEquals(2, g1.getSeen().size());
        assertTrue(g1.hasSeen(s1.getId()));
        assertTrue(g1.hasSeen(s5Down.getId()));
    }

    @Test
    public void testCompareByState() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g2 = g2.update(s1.getId(), s1.status(GossipNodeStatus.UP));

        assertSame(ComparisonResult.BEFORE, g1.compare(g2));
        assertSame(ComparisonResult.AFTER, g2.compare(g1));

        assertSame(ComparisonResult.CONCURRENT, g1.update(s1.getId(), s2.status(GossipNodeStatus.UP)).compare(g2));
        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1.update(s1.getId(), s2.status(GossipNodeStatus.UP))));

        g1 = g1.update(s1.getId(), s1.status(GossipNodeStatus.UP));

        assertSame(ComparisonResult.SAME, g1.compare(g2));
        assertSame(ComparisonResult.SAME, g2.compare(g1));
    }

    @Test
    public void testMergeByState() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1.status(GossipNodeStatus.UP))
            .update(s1.getId(), s2.status(GossipNodeStatus.JOINING))
            .update(s1.getId(), s3.status(GossipNodeStatus.UP));

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));

        Gossip g3 = g1.merge(s1.getId(), g2);

        assertSame(ComparisonResult.BEFORE, g1.compare(g3));
        assertSame(ComparisonResult.BEFORE, g2.compare(g3));

        assertSame(ComparisonResult.AFTER, g3.compare(g1));
        assertSame(ComparisonResult.AFTER, g3.compare(g2));

        assertSame(GossipNodeStatus.UP, g3.getMember(s1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(s2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(s3.getId()).getStatus());

        Gossip g4 = g2.merge(s1.getId(), g1);

        assertSame(ComparisonResult.SAME, g3.compare(g4));
        assertSame(ComparisonResult.SAME, g4.compare(g3));
    }

    @Test
    public void testMergeWithMaxJoinOrder() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3)
            .maxJoinOrder(10);

        g2 = g2.update(s1.getId(), s1.status(GossipNodeStatus.UP))
            .update(s1.getId(), s2.status(GossipNodeStatus.JOINING))
            .update(s1.getId(), s3.status(GossipNodeStatus.UP))
            .maxJoinOrder(100);

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));

        Gossip g3 = g1.merge(s1.getId(), g2);

        assertSame(ComparisonResult.BEFORE, g1.compare(g3));
        assertSame(ComparisonResult.BEFORE, g2.compare(g3));

        assertSame(ComparisonResult.AFTER, g3.compare(g1));
        assertSame(ComparisonResult.AFTER, g3.compare(g2));

        assertSame(GossipNodeStatus.UP, g3.getMember(s1.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(s2.getId()).getStatus());
        assertSame(GossipNodeStatus.UP, g3.getMember(s3.getId()).getStatus());

        assertEquals(100, g3.getMaxJoinOrder());

        Gossip g4 = g2.merge(s1.getId(), g1);

        assertSame(ComparisonResult.SAME, g3.compare(g4));
        assertSame(ComparisonResult.SAME, g4.compare(g3));

        assertEquals(100, g4.getMaxJoinOrder());
    }

    @Test
    public void testCompareByMembers() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s4 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2);

        assertSame(ComparisonResult.AFTER, g1.compare(g2));
        assertSame(ComparisonResult.BEFORE, g2.compare(g1));

        g2 = g2.update(s1.getId(), s4);

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));
        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1));
    }

    @Test
    public void testCompareByMembersWithRemoved() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s4 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s5 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .purge(s1.getId(), Collections.singleton(s3.getId()));

        assertSame(ComparisonResult.BEFORE, g1.compare(g2));
        assertSame(ComparisonResult.AFTER, g2.compare(g1));

        g2 = g2.update(s1.getId(), s4);

        assertSame(ComparisonResult.BEFORE, g1.compare(g2));
        assertSame(ComparisonResult.AFTER, g2.compare(g1));

        g1 = g1.update(s1.getId(), s5);

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));
        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1));
    }

    @Test
    public void testMergeByMembers() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s4 = newState(GossipNodeStatus.JOINING);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s4);

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));
        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1));

        Gossip g3 = g1.merge(s1.getId(), g2);

        assertSame(ComparisonResult.BEFORE, g1.compare(g3));
        assertSame(ComparisonResult.BEFORE, g2.compare(g3));

        assertSame(ComparisonResult.AFTER, g3.compare(g1));
        assertSame(ComparisonResult.AFTER, g3.compare(g2));

        assertTrue(g3.hasMember(s1.getId()));
        assertTrue(g3.hasMember(s2.getId()));
        assertTrue(g3.hasMember(s3.getId()));
        assertTrue(g3.hasMember(s4.getId()));

        Gossip g4 = g2.merge(s1.getId(), g1);

        assertSame(ComparisonResult.SAME, g3.compare(g4));
        assertSame(ComparisonResult.SAME, g4.compare(g3));
    }

    @Test
    public void testCompareBySuspected() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertSame(ComparisonResult.SAME, g1.compare(g2));

        g2 = g2.update(s1.getId(), s1.suspected(s2.getId()));

        assertSame(ComparisonResult.BEFORE, g1.compare(g2));
        assertSame(ComparisonResult.AFTER, g2.compare(g1));

        assertSame(ComparisonResult.CONCURRENT,
            g1.update(s1.getId(), s2.suspected(s1.getId())).compare(g2));
        assertSame(ComparisonResult.CONCURRENT,
            g2.compare(g1.update(s1.getId(), s2.suspected(s1.getId()))));
    }

    @Test
    public void testClearRemovedSuspected() throws Exception {
        Gossip g1 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.getId(), s1.suspected(s3.getId()))
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertEquals(1, g1.getAllSuspected().size());

        Gossip g2 = g1.purge(s2.getId(), Collections.singleton(s3.getId()));

        g1 = g1.update(s1.getId(), s2.status(GossipNodeStatus.LEAVING));

        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1));

        Gossip g3 = g2.merge(s1.getId(), g1);

        say(g1);
        say(g2);
        say(g3);

        assertTrue(g3.getAllSuspected().isEmpty());
    }

    @Test
    public void testMergeBySuspected() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.getId(), s1.suspected(s2.getId()))
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        g2 = g2.update(s1.getId(), s1)
            .update(s1.getId(), s2.suspected(s1.getId()))
            .update(s1.getId(), s3);

        assertSame(ComparisonResult.CONCURRENT, g1.compare(g2));
        assertSame(ComparisonResult.CONCURRENT, g2.compare(g1));

        Gossip g3 = g1.merge(s1.getId(), g2);

        assertSame(ComparisonResult.BEFORE, g1.compare(g3));
        assertSame(ComparisonResult.BEFORE, g2.compare(g3));

        assertSame(ComparisonResult.AFTER, g3.compare(g1));
        assertSame(ComparisonResult.AFTER, g3.compare(g2));

        assertTrue(g3.getMember(s1.getId()).isSuspected(s2.getId()));
        assertTrue(g3.getMember(s2.getId()).isSuspected(s1.getId()));
        assertFalse(g3.getMember(s3.getId()).hasSuspected());

        Gossip g4 = g2.merge(s1.getId(), g1);

        assertSame(ComparisonResult.SAME, g3.compare(g4));
        assertSame(ComparisonResult.SAME, g4.compare(g3));
    }

    @Test
    public void testConvergence() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId());

        assertFalse(g.isConvergent());

        g = g.seen(s3.getId());

        assertTrue(g.isConvergent());

        g = g.update(s1.getId(), s1.status(GossipNodeStatus.UP))
            .update(s1.getId(), s2.status(GossipNodeStatus.UP))
            .update(s1.getId(), s3.status(GossipNodeStatus.UP));

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        assertTrue(g.isConvergent());

        g = g.update(s1.getId(), s1.status(GossipNodeStatus.LEAVING))
            .update(s1.getId(), s2.status(GossipNodeStatus.LEAVING))
            .update(s1.getId(), s3.status(GossipNodeStatus.LEAVING));

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        g = g.update(s1.getId(), s1.status(GossipNodeStatus.DOWN))
            .update(s1.getId(), s2.status(GossipNodeStatus.DOWN))
            .update(s1.getId(), s3.status(GossipNodeStatus.DOWN));

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        assertTrue(g.isConvergent());
    }

    @Test
    public void testConvergenceWhenSuspectedDown() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.DOWN);

        Gossip g = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        assertTrue(g.isConvergent());

        // Suspected DOWN.
        g = g.update(s1.getId(), s1.suspected(s3.getId()));

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId());

        // Not seen by DOWN but still convergent.
        assertTrue(g.isConvergent());
    }

    @Test
    public void testNoConvergenceWhenSuspected() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        assertTrue(g.isConvergent());

        // Suspected.
        g = g.update(s1.getId(), s1.suspected(s2.getId()));

        assertFalse(g.isConvergent());

        g = g.seen(s3.getId());

        assertFalse(g.isConvergent());

        // Unsuspected.
        g = g.update(s1.getId(), s1.unsuspected(s2.getId()));

        assertFalse(g.isConvergent());

        g = g.seen(s2.getId()).seen(s3.getId());

        assertTrue(g.isConvergent());
    }

    @Test
    public void testSuspected() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3);

        assertFalse(g.isSuspected(s1.getId()));
        assertFalse(g.isSuspected(s2.getId()));
        assertFalse(g.isSuspected(s3.getId()));
        assertTrue(g.getSuspectedView().isEmpty());

        g = g.update(s1.getId(), s1.suspected(s2.getId()));

        assertFalse(g.isSuspected(s1.getId()));
        assertTrue(g.isSuspected(s2.getId()));
        assertFalse(g.isSuspected(s3.getId()));
        assertFalse(g.getSuspectedView().isEmpty());
        assertEquals(1, g.getSuspectedView().getSuspected().size());
        assertEquals(Collections.singleton(s1.getId()), g.getSuspectedView().getSuspecting(s2.getId()));

        g = g.update(s1.getId(), s3.suspected(s2.getId()));

        assertFalse(g.isSuspected(s1.getId()));
        assertTrue(g.isSuspected(s2.getId()));
        assertFalse(g.isSuspected(s3.getId()));
        assertFalse(g.getSuspectedView().isEmpty());
        assertEquals(1, g.getSuspectedView().getSuspected().size());
        assertEquals(toSet(s1.getId(), s3.getId()), g.getSuspectedView().getSuspecting(s2.getId()));
    }

    @Test
    public void testPurge() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g1 = new Gossip()
            .update(s1.getId(), s1)
            .update(s1.getId(), s2)
            .update(s1.getId(), s3)
            .seen(s2.getId())
            .seen(s3.getId());

        long v1 = g1.getVersion();

        g1 = g1.purge(s1.getId(), toSet(s2.getId()));

        assertEquals(v1 + 1, g1.getVersion());
        assertEquals(2, g1.getMembers().size());
        assertFalse(g1.hasMember(s2.getId()));
        assertFalse(g1.hasSeen(s2.getId()));
        assertTrue(g1.getRemoved().contains(s2.getId()));

        long v2 = g1.getVersion();

        g1 = g1.purge(s1.getId(), toSet(s3.getId()));

        assertEquals(v2 + 1, g1.getVersion());
        assertEquals(1, g1.getMembers().size());
        assertFalse(g1.hasMember(s3.getId()));
        assertFalse(g1.hasSeen(s3.getId()));
        assertTrue(g1.getRemoved().contains(s3.getId()));
        assertFalse(g1.getRemoved().contains(s2.getId()));
    }

    private GossipNodeState newState(GossipNodeStatus status) throws Exception {
        ClusterNode node = newNode();

        return new GossipNodeState(node, status);
    }
}
