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
import io.hekate.cluster.ClusterNodeId;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class GossipNodesDeathWatchTest extends HekateTestBase {
    private ClusterNodeId id;

    private GossipNodeState n1;

    private GossipNodeState n2;

    private GossipNodeState n3;

    private GossipNodesDeathWatch watch;

    private Gossip gossip;

    @Before
    public void setUp() throws Exception {
        ClusterNode node = newNode();

        id = node.id();

        n1 = new GossipNodeState(newNode(), GossipNodeStatus.UP);
        n2 = new GossipNodeState(newNode(), GossipNodeStatus.UP);
        n3 = new GossipNodeState(newNode(), GossipNodeStatus.UP);

        gossip = new Gossip().update(id, new GossipNodeState(node, GossipNodeStatus.UP))
            .update(id, n1)
            .update(id, n2)
            .update(id, n3);

        watch = new GossipNodesDeathWatch(node.id(), 1, 0);
    }

    @Test
    public void testNoSuspect() throws Exception {
        watch.update(gossip);

        assertTrue(watch.terminateNodes().isEmpty());
    }

    @Test
    public void testSuspectUnSuspect() throws Exception {
        gossip = gossip.update(id, n1.suspect(n2.id()));

        watch.update(gossip);

        gossip = gossip.update(id, n1.unsuspect(n2.id()));

        watch.update(gossip);

        assertTrue(watch.terminateNodes().isEmpty());
    }

    @Test
    public void testTerminateSingle() throws Exception {
        gossip = gossip.update(id, n1.suspect(n2.id()));

        watch.update(gossip);

        assertTrue(watch.terminateNodes().contains(n2.id()));
    }

    @Test
    public void testTerminateMultiple() throws Exception {
        gossip = gossip.update(id, n1.suspect(toSet(n2.id(), n3.id())));

        watch.update(gossip);

        List<ClusterNodeId> terminated = watch.terminateNodes();

        assertTrue(terminated.contains(n2.id()));
        assertTrue(terminated.contains(n3.id()));
    }

    @Test
    public void testNotTerminateLocalNode() throws Exception {
        gossip = gossip.update(id, n1.suspect(id));

        watch.update(gossip);

        assertTrue(watch.terminateNodes().isEmpty());
    }
}
