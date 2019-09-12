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

        assertEquals(0, gossip.version());
        assertEquals(0, gossip.maxJoinOrder());
        assertTrue(gossip.allSuspected().isEmpty());
        assertTrue(gossip.members().isEmpty());
        assertTrue(gossip.seen().isEmpty());
        assertTrue(gossip.allSuspected().isEmpty());
        assertFalse(gossip.hasMembers());
        assertNotNull(gossip.stream());

        GossipSuspectView suspectedView = gossip.suspectView();

        assertNotNull(suspectedView);
        assertTrue(suspectedView.isEmpty());
        assertTrue(suspectedView.suspected().isEmpty());
    }

    @Test
    public void testNonEmpty() throws Exception {
        Gossip g1 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g2 = g1.update(s1.id(), s1);

        assertNotSame(g1, g2);
        assertTrue(g2.hasMembers());
        assertEquals(1, g2.members().size());
        assertEquals(1, g2.seen().size());
        assertTrue(g2.hasSeen(s1.id()));
        assertTrue(g2.hasMember(s1.id()));
        assertEquals(s1, g2.member(s1.id()));

        Gossip g3 = g2.update(s1.id(), s2);

        assertNotSame(g2, g3);
        assertTrue(g3.hasMembers());
        assertEquals(2, g3.members().size());
        assertEquals(1, g3.seen().size());
        assertTrue(g3.hasSeen(s1.id()));
        assertFalse(g3.hasSeen(s2.id()));
        assertTrue(g3.hasMember(s1.id()));
        assertTrue(g3.hasMember(s2.id()));
        assertEquals(s1, g3.member(s1.id()));
        assertEquals(s2, g3.member(s2.id()));

        Gossip g4 = g3.update(s1.id(), s3);

        assertNotSame(g3, g4);
        assertTrue(g4.hasMembers());
        assertEquals(3, g4.members().size());
        assertEquals(1, g4.seen().size());
        assertTrue(g4.hasSeen(s1.id()));
        assertFalse(g4.hasSeen(s2.id()));
        assertFalse(g4.hasSeen(s3.id()));
        assertTrue(g4.hasMember(s1.id()));
        assertTrue(g4.hasMember(s2.id()));
        assertTrue(g4.hasMember(s3.id()));
        assertEquals(s1, g4.member(s1.id()));
        assertEquals(s2, g4.member(s2.id()));
        assertEquals(s3, g4.member(s3.id()));

        Gossip g5 = g4.seen(s2.id()).seen(s3.id());

        assertNotSame(g4, g5);
        assertTrue(g5.hasMembers());
        assertEquals(3, g5.members().size());
        assertEquals(3, g5.seen().size());
        assertTrue(g5.hasSeen(s1.id()));
        assertTrue(g5.hasSeen(s2.id()));
        assertTrue(g5.hasSeen(s3.id()));
        assertTrue(g5.hasMember(s1.id()));
        assertTrue(g5.hasMember(s2.id()));
        assertTrue(g5.hasMember(s3.id()));
        assertEquals(s1, g5.member(s1.id()));
        assertEquals(s2, g5.member(s2.id()));
        assertEquals(s3, g5.member(s3.id()));

        Gossip g6 = g5.purge(s1.id(), Collections.singleton(s3.id()));

        assertNotSame(g5, g6);
        assertTrue(g6.hasMembers());
        assertEquals(2, g6.members().size());
        assertEquals(1, g6.seen().size());
        assertTrue(g6.hasSeen(s1.id()));
        assertFalse(g6.hasSeen(s2.id()));
        assertFalse(g6.hasSeen(s3.id()));
        assertTrue(g6.hasMember(s1.id()));
        assertTrue(g6.hasMember(s2.id()));
        assertFalse(g6.hasMember(s3.id()));
        assertEquals(s1, g6.member(s1.id()));
        assertEquals(s2, g6.member(s2.id()));
        assertNull(g6.member(s3.id()));

        Gossip g7 = g6.maxJoinOrder(1000);

        assertNotSame(g5, g6);
        assertEquals(1000, g7.maxJoinOrder());
    }

    @Test
    public void testSeen() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g1 = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertEquals(1, g1.seen().size());
        assertTrue(g1.hasSeen(s1.id()));
        assertFalse(g1.hasSeen(s2.id()));
        assertFalse(g1.hasSeen(s3.id()));

        assertSame(g1, g1.seen(s1.id()));

        Gossip g2 = g1.seen(s2.id());

        assertNotSame(g1, g2);
        assertEquals(2, g2.seen().size());
        assertTrue(g2.hasSeen(s1.id()));
        assertTrue(g2.hasSeen(s2.id()));
        assertFalse(g1.hasSeen(s3.id()));

        Gossip g3 = g2.seen(toSet(s1.id(), s2.id(), s3.id()));

        assertNotSame(g2, g3);
        assertEquals(3, g3.seen().size());
        assertTrue(g3.hasSeen(s1.id()));
        assertTrue(g3.hasSeen(s2.id()));
        assertTrue(g3.hasSeen(s3.id()));

        assertTrue(g3.hasSeenAll(toSet(s1.id(), s2.id(), s3.id())));
        assertFalse(g3.hasSeenAll(toSet(s1.id(), s2.id(), s3.id(), newNodeId())));

        assertSame(g3, g3.seen(toSet(s1.id(), s2.id(), s3.id())));
    }

    @Test
    public void testSeenInherit() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);
        GossipNodeState s4 = newState(GossipNodeStatus.UP);
        GossipNodeState s5Down = newState(GossipNodeStatus.DOWN);

        Gossip g1 = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        Gossip g2 = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3)
            .update(s1.id(), s4)
            .update(s1.id(), s5Down)
            .seen(s2.id())
            .seen(s3.id())
            .seen(s4.id())
            .seen(s5Down.id());

        g1 = g1.inheritSeen(s1.id(), g2);

        assertEquals(1, g1.seen().size());
        assertTrue(g1.hasSeen(s1.id()));
        assertFalse(g1.hasSeen(s5Down.id()));

        g1 = g1.update(s1.id(), s5Down);

        assertEquals(1, g1.seen().size());
        assertTrue(g1.hasSeen(s1.id()));
        assertFalse(g1.hasSeen(s5Down.id()));

        g1 = g1.inheritSeen(s1.id(), g2);

        assertEquals(2, g1.seen().size());
        assertTrue(g1.hasSeen(s1.id()));
        assertTrue(g1.hasSeen(s5Down.id()));
    }

    @Test
    public void testCompareByState() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g2 = g2.update(s1.id(), s1.status(GossipNodeStatus.UP));

        assertSame(GossipPrecedence.BEFORE, g1.compare(g2));
        assertSame(GossipPrecedence.AFTER, g2.compare(g1));

        assertSame(GossipPrecedence.CONCURRENT, g1.update(s1.id(), s2.status(GossipNodeStatus.UP)).compare(g2));
        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1.update(s1.id(), s2.status(GossipNodeStatus.UP))));

        g1 = g1.update(s1.id(), s1.status(GossipNodeStatus.UP));

        assertSame(GossipPrecedence.SAME, g1.compare(g2));
        assertSame(GossipPrecedence.SAME, g2.compare(g1));
    }

    @Test
    public void testMergeByState() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1.status(GossipNodeStatus.UP))
            .update(s1.id(), s2.status(GossipNodeStatus.JOINING))
            .update(s1.id(), s3.status(GossipNodeStatus.UP));

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));

        Gossip g3 = g1.merge(s1.id(), g2);

        assertSame(GossipPrecedence.BEFORE, g1.compare(g3));
        assertSame(GossipPrecedence.BEFORE, g2.compare(g3));

        assertSame(GossipPrecedence.AFTER, g3.compare(g1));
        assertSame(GossipPrecedence.AFTER, g3.compare(g2));

        assertSame(GossipNodeStatus.UP, g3.member(s1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(s2.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(s3.id()).status());

        Gossip g4 = g2.merge(s1.id(), g1);

        assertSame(GossipPrecedence.SAME, g3.compare(g4));
        assertSame(GossipPrecedence.SAME, g4.compare(g3));
    }

    @Test
    public void testMergeWithMaxJoinOrder() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3)
            .maxJoinOrder(10);

        g2 = g2.update(s1.id(), s1.status(GossipNodeStatus.UP))
            .update(s1.id(), s2.status(GossipNodeStatus.JOINING))
            .update(s1.id(), s3.status(GossipNodeStatus.UP))
            .maxJoinOrder(100);

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));

        Gossip g3 = g1.merge(s1.id(), g2);

        assertSame(GossipPrecedence.BEFORE, g1.compare(g3));
        assertSame(GossipPrecedence.BEFORE, g2.compare(g3));

        assertSame(GossipPrecedence.AFTER, g3.compare(g1));
        assertSame(GossipPrecedence.AFTER, g3.compare(g2));

        assertSame(GossipNodeStatus.UP, g3.member(s1.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(s2.id()).status());
        assertSame(GossipNodeStatus.UP, g3.member(s3.id()).status());

        assertEquals(100, g3.maxJoinOrder());

        Gossip g4 = g2.merge(s1.id(), g1);

        assertSame(GossipPrecedence.SAME, g3.compare(g4));
        assertSame(GossipPrecedence.SAME, g4.compare(g3));

        assertEquals(100, g4.maxJoinOrder());
    }

    @Test
    public void testCompareByMembers() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s4 = newState(GossipNodeStatus.JOINING);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2);

        assertSame(GossipPrecedence.AFTER, g1.compare(g2));
        assertSame(GossipPrecedence.BEFORE, g2.compare(g1));

        g2 = g2.update(s1.id(), s4);

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));
        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1));
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

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2)
            .purge(s1.id(), Collections.singleton(s3.id()));

        assertSame(GossipPrecedence.BEFORE, g1.compare(g2));
        assertSame(GossipPrecedence.AFTER, g2.compare(g1));

        g2 = g2.update(s1.id(), s4);

        assertSame(GossipPrecedence.BEFORE, g1.compare(g2));
        assertSame(GossipPrecedence.AFTER, g2.compare(g1));

        g1 = g1.update(s1.id(), s5);

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));
        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1));
    }

    @Test
    public void testMergeByMembers() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s4 = newState(GossipNodeStatus.JOINING);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s4);

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));
        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1));

        Gossip g3 = g1.merge(s1.id(), g2);

        assertSame(GossipPrecedence.BEFORE, g1.compare(g3));
        assertSame(GossipPrecedence.BEFORE, g2.compare(g3));

        assertSame(GossipPrecedence.AFTER, g3.compare(g1));
        assertSame(GossipPrecedence.AFTER, g3.compare(g2));

        assertTrue(g3.hasMember(s1.id()));
        assertTrue(g3.hasMember(s2.id()));
        assertTrue(g3.hasMember(s3.id()));
        assertTrue(g3.hasMember(s4.id()));

        Gossip g4 = g2.merge(s1.id(), g1);

        assertSame(GossipPrecedence.SAME, g3.compare(g4));
        assertSame(GossipPrecedence.SAME, g4.compare(g3));
    }

    @Test
    public void testCompareBySuspected() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertSame(GossipPrecedence.SAME, g1.compare(g2));

        g2 = g2.update(s1.id(), s1.suspect(s2.id()));

        assertSame(GossipPrecedence.BEFORE, g1.compare(g2));
        assertSame(GossipPrecedence.AFTER, g2.compare(g1));

        assertSame(GossipPrecedence.CONCURRENT,
            g1.update(s1.id(), s2.suspect(s1.id())).compare(g2));
        assertSame(GossipPrecedence.CONCURRENT,
            g2.compare(g1.update(s1.id(), s2.suspect(s1.id()))));
    }

    @Test
    public void testClearRemovedSuspected() throws Exception {
        Gossip g1 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.id(), s1.suspect(s3.id()))
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertEquals(1, g1.allSuspected().size());

        Gossip g2 = g1.purge(s2.id(), Collections.singleton(s3.id()));

        g1 = g1.update(s1.id(), s2.status(GossipNodeStatus.LEAVING));

        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1));

        Gossip g3 = g2.merge(s1.id(), g1);

        say(g1);
        say(g2);
        say(g3);

        assertTrue(g3.allSuspected().isEmpty());
    }

    @Test
    public void testMergeBySuspected() throws Exception {
        Gossip g1 = new Gossip();
        Gossip g2 = new Gossip();

        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        g1 = g1.update(s1.id(), s1.suspect(s2.id()))
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        g2 = g2.update(s1.id(), s1)
            .update(s1.id(), s2.suspect(s1.id()))
            .update(s1.id(), s3);

        assertSame(GossipPrecedence.CONCURRENT, g1.compare(g2));
        assertSame(GossipPrecedence.CONCURRENT, g2.compare(g1));

        Gossip g3 = g1.merge(s1.id(), g2);

        assertSame(GossipPrecedence.BEFORE, g1.compare(g3));
        assertSame(GossipPrecedence.BEFORE, g2.compare(g3));

        assertSame(GossipPrecedence.AFTER, g3.compare(g1));
        assertSame(GossipPrecedence.AFTER, g3.compare(g2));

        assertTrue(g3.member(s1.id()).isSuspected(s2.id()));
        assertTrue(g3.member(s2.id()).isSuspected(s1.id()));
        assertFalse(g3.member(s3.id()).hasSuspected());

        Gossip g4 = g2.merge(s1.id(), g1);

        assertSame(GossipPrecedence.SAME, g3.compare(g4));
        assertSame(GossipPrecedence.SAME, g4.compare(g3));
    }

    @Test
    public void testConvergence() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.id());

        assertFalse(g.isConvergent());

        g = g.seen(s3.id());

        assertTrue(g.isConvergent());

        g = g.update(s1.id(), s1.status(GossipNodeStatus.UP))
            .update(s1.id(), s2.status(GossipNodeStatus.UP))
            .update(s1.id(), s3.status(GossipNodeStatus.UP));

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        assertTrue(g.isConvergent());

        g = g.update(s1.id(), s1.status(GossipNodeStatus.LEAVING))
            .update(s1.id(), s2.status(GossipNodeStatus.LEAVING))
            .update(s1.id(), s3.status(GossipNodeStatus.LEAVING));

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        g = g.update(s1.id(), s1.status(GossipNodeStatus.DOWN))
            .update(s1.id(), s2.status(GossipNodeStatus.DOWN))
            .update(s1.id(), s3.status(GossipNodeStatus.DOWN));

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        assertTrue(g.isConvergent());
    }

    @Test
    public void testConvergenceWhenSuspectedDown() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.DOWN);

        Gossip g = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        assertTrue(g.isConvergent());

        // Suspected DOWN.
        g = g.update(s1.id(), s1.suspect(s3.id()));

        assertFalse(g.isConvergent());

        g = g.seen(s2.id());

        // Not seen by DOWN but still convergent.
        assertTrue(g.isConvergent());
    }

    @Test
    public void testNoConvergenceWhenSuspected() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        assertTrue(g.isConvergent());

        // Suspected.
        g = g.update(s1.id(), s1.suspect(s2.id()));

        assertFalse(g.isConvergent());

        g = g.seen(s3.id());

        assertFalse(g.isConvergent());

        // Unsuspected.
        g = g.update(s1.id(), s1.unsuspect(s2.id()));

        assertFalse(g.isConvergent());

        g = g.seen(s2.id()).seen(s3.id());

        assertTrue(g.isConvergent());
    }

    @Test
    public void testSuspected() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s2 = newState(GossipNodeStatus.JOINING);
        GossipNodeState s3 = newState(GossipNodeStatus.JOINING);

        Gossip g = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertFalse(g.isSuspected(s1.id()));
        assertFalse(g.isSuspected(s2.id()));
        assertFalse(g.isSuspected(s3.id()));
        assertTrue(g.suspectView().isEmpty());

        g = g.update(s1.id(), s1.suspect(s2.id()));

        assertFalse(g.isSuspected(s1.id()));
        assertTrue(g.isSuspected(s2.id()));
        assertFalse(g.isSuspected(s3.id()));
        assertFalse(g.suspectView().isEmpty());
        assertEquals(1, g.suspectView().suspected().size());
        assertEquals(Collections.singleton(s1.id()), g.suspectView().suspecting(s2.id()));

        g = g.update(s1.id(), s3.suspect(s2.id()));

        assertFalse(g.isSuspected(s1.id()));
        assertTrue(g.isSuspected(s2.id()));
        assertFalse(g.isSuspected(s3.id()));
        assertFalse(g.suspectView().isEmpty());
        assertEquals(1, g.suspectView().suspected().size());
        assertEquals(toSet(s1.id(), s3.id()), g.suspectView().suspecting(s2.id()));
    }

    @Test
    public void testPurge() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.UP);

        Gossip g1 = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3)
            .seen(s2.id())
            .seen(s3.id());

        long v1 = g1.version();

        g1 = g1.purge(s1.id(), toSet(s2.id()));

        assertEquals(v1 + 1, g1.version());
        assertEquals(2, g1.members().size());
        assertFalse(g1.hasMember(s2.id()));
        assertFalse(g1.hasSeen(s2.id()));
        assertTrue(g1.removed().contains(s2.id()));

        long v2 = g1.version();

        g1 = g1.purge(s1.id(), toSet(s3.id()));

        assertEquals(v2 + 1, g1.version());
        assertEquals(1, g1.members().size());
        assertFalse(g1.hasMember(s3.id()));
        assertFalse(g1.hasSeen(s3.id()));
        assertTrue(g1.removed().contains(s3.id()));
        assertFalse(g1.removed().contains(s2.id()));
    }

    @Test
    public void testIsDown() throws Exception {
        GossipNodeState s1 = newState(GossipNodeStatus.UP);
        GossipNodeState s2 = newState(GossipNodeStatus.UP);
        GossipNodeState s3 = newState(GossipNodeStatus.DOWN);

        Gossip g = new Gossip()
            .update(s1.id(), s1)
            .update(s1.id(), s2)
            .update(s1.id(), s3);

        assertFalse(g.isDown(s1.id()));
        assertFalse(g.isDown(s2.id()));
        assertTrue(g.isDown(s3.id()));

        g = g.purge(s1.id(), Collections.singleton(s3.id()));

        assertFalse(g.members().containsKey(s3.id()));

        assertFalse(g.isDown(s1.id()));
        assertFalse(g.isDown(s2.id()));
        assertTrue(g.isDown(s3.id()));
    }

    private GossipNodeState newState(GossipNodeStatus status) throws Exception {
        ClusterNode node = newNode();

        return new GossipNodeState(node, status);
    }
}
