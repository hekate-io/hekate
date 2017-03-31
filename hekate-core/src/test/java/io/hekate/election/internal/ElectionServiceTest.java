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

package io.hekate.election.internal;

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateTestInstance;
import io.hekate.election.Candidate;
import io.hekate.election.CandidateConfig;
import io.hekate.election.ElectionService;
import io.hekate.election.ElectionServiceFactory;
import io.hekate.election.FollowerContext;
import io.hekate.election.LeaderContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ElectionServiceTest extends HekateInstanceContextTestBase {
    private static final String GROUP = "GROUP";

    public ElectionServiceTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testConcurrentElectionWithLeave() throws Exception {
        doTestConcurrentElection(node -> {
            say("Leave " + node.getNode());

            node.leaveAsync().join();
        });
    }

    @Test
    public void testConcurrentElectionWithTerminate() throws Exception {
        disableNodeFailurePostCheck();

        doTestConcurrentElection(node -> {
            say("Terminate " + node.getNode());

            node.terminateAsync().join();
        });
    }

    @Test
    public void testYieldLeadershipSingleNode() throws Exception {
        HekateTestInstance node = createInstanceWithElection().join();

        CandidateMock candidate = getCandidate(node);

        for (int i = 0; i < 3; i++) {
            assertEquals(node.getNode(), get(node.get(ElectionService.class).leader(GROUP)));

            candidate.yieldLeadership();

            candidate.awaitForBecomeLeader();

            assertEquals(node.getNode(), get(node.get(ElectionService.class).leader(GROUP)));
        }
    }

    @Test
    public void testYieldLeadershipSameNodes() throws Exception {
        // Start leader.
        HekateTestInstance node1 = createInstanceWithElection().join();

        get(node1.get(ElectionService.class).leader(GROUP));

        // Start follower.
        HekateTestInstance node2 = createInstanceWithElection().join();

        for (int i = 0; i < 5; i++) {
            say("Iteration: " + i);

            // Make sure that node1 is the leader.
            assertEquals(node1.getNode(), get(node1.get(ElectionService.class).leader(GROUP)));
            assertEquals(node1.getNode(), get(node2.get(ElectionService.class).leader(GROUP)));

            CandidateMock candidate1 = getCandidate(node1);
            CandidateMock candidate2 = getCandidate(node2);

            // Yield leadership.
            candidate1.yieldLeadership();

            // Await for leadership change.
            candidate1.awaitForLeaderChange(node1.getNode());
            candidate2.awaitForLeaderChange(node1.getNode());

            // Verify that node2 became the leader.
            assertTrue(candidate1.isFollower());
            assertTrue(candidate2.isLeader());

            awaitForLeaderFuture(node2.getNode(), node1);
            awaitForLeaderFuture(node2.getNode(), node2);

            // Invert nodes for the next iteration.
            HekateTestInstance temp = node1;

            node1 = node2;
            node2 = temp;
        }
    }

    @Test
    public void testYieldLeadershipNewNodes() throws Exception {
        repeat(5, i -> {
            // Start leader.
            HekateTestInstance node1 = createInstanceWithElection().join();

            get(node1.get(ElectionService.class).leader(GROUP));

            // Start follower.
            HekateTestInstance node2 = createInstanceWithElection().join();

            // Make sure that node1 is the leader.
            assertEquals(node1.getNode(), get(node1.get(ElectionService.class).leader(GROUP)));
            assertEquals(node1.getNode(), get(node2.get(ElectionService.class).leader(GROUP)));

            CandidateMock candidate1 = getCandidate(node1);
            CandidateMock candidate2 = getCandidate(node2);

            // Yield leadership.
            candidate1.yieldLeadership();

            // Await for leadership change.
            candidate1.awaitForLeaderChange(node1.getNode());
            candidate2.awaitForLeaderChange(node1.getNode());

            // Verify that node2 became the leader.
            assertTrue(candidate1.isFollower());
            assertTrue(candidate2.isLeader());

            awaitForLeaderFuture(node2.getNode(), node1);
            awaitForLeaderFuture(node2.getNode(), node2);

            node1.leave();
            node2.leave();
        });
    }

    @Test
    public void testLeaderLeave() throws Exception {
        // Start leader.
        HekateTestInstance leader = createInstanceWithElection().join();

        get(leader.get(ElectionService.class).leader(GROUP));

        for (int i = 0; i < 5; i++) {
            say("Iteration: " + i);

            // Start follower.
            HekateTestInstance follower = createInstanceWithElection().join();

            // Verify leader.
            assertEquals(leader.getNode(), get(follower.get(ElectionService.class).leader(GROUP)));

            CandidateMock followerCandidate = getCandidate(follower);

            ClusterNode oldLeader = leader.getNode();

            // Leader leaves the cluster.
            leader.leave();

            // Await for leadership change.
            followerCandidate.awaitForLeaderChange(oldLeader);

            // Verify that follower became the new leader.
            assertTrue(followerCandidate.isLeader());

            awaitForLeaderFuture(follower.getNode(), follower);

            // Old follower becomes a new leader of the next iteration.
            leader = follower;
        }
    }

    @Test
    public void testClusterChange() throws Exception {
        List<HekateTestInstance> nodes = new ArrayList<>();

        repeat(5, i -> {
            // Start few more nodes.
            nodes.add(createInstanceWithElection().join());
            nodes.add(createInstanceWithElection().join());

            // Check that only single leader exists.
            Set<ClusterNode> leaders = new HashSet<>();

            for (HekateTestInstance node : nodes) {
                ClusterNode leader = get(node.get(ElectionService.class).leader(GROUP));

                assertTrue(nodes.stream().anyMatch(n -> n.getNode().equals(leader)));

                leaders.add(leader);

                assertEquals(leaders.toString(), 1, leaders.size());

                CandidateMock candidate = getCandidate(node);

                if (leader.equals(node.getNode())) {
                    assertTrue(candidate.isLeader());
                } else {
                    assertTrue(candidate.isFollower());
                    assertEquals(leader, candidate.getLastLeader());
                }
            }

            // Stop leader.
            HekateTestInstance oldLeader = nodes.stream().filter(n -> leaders.contains(n.getNode())).findFirst().orElse(null);

            ClusterNode oldLeaderNode = oldLeader.getNode();

            nodes.remove(oldLeader);

            CandidateMock oldLeaderCandidate = getCandidate(oldLeader);

            say("Stopping leader " + oldLeader.getNode());

            oldLeader.leave();

            oldLeaderCandidate.awaitTermination();

            for (HekateTestInstance node : nodes) {
                say("Checking " + node.getNode());

                getCandidate(node).awaitForLeaderChange(oldLeaderNode);
            }
        });
    }

    private void doTestConcurrentElection(Consumer<HekateTestInstance> nodeStopAction) throws Exception {
        repeat(5, i -> {
            AtomicInteger leaderIdx = new AtomicInteger();
            AtomicReference<AssertionError> err = new AtomicReference<>();

            List<HekateTestInstance> nodes = new ArrayList<>();

            for (int j = 0; j < 2; j++) {
                int idx = j + 1;

                nodes.add(createInstanceWithElection(new Candidate() {
                    @Override
                    public void becomeLeader(LeaderContext ctx) {
                        say("Leader " + idx);

                        try {
                            assertEquals("On node " + idx, 0, leaderIdx.get());
                            assertTrue(leaderIdx.compareAndSet(0, idx));
                        } catch (AssertionError e) {
                            say("Fail: " + e);

                            err.compareAndSet(null, e);
                        }
                    }

                    @Override
                    public void becomeFollower(FollowerContext ctx) {
                        say("Follower " + idx);
                    }

                    @Override
                    public void terminate() {
                        leaderIdx.compareAndSet(idx, 0);

                        say("Terminated " + idx);
                    }
                }).join());
            }

            nodes.forEach(nodeStopAction);

            if (err.get() != null) {
                throw err.get();
            }
        });
    }

    private void awaitForLeaderFuture(ClusterNode expectedLeader, HekateTestInstance node) throws Exception {
        busyWait("leader " + expectedLeader, () -> {
            ClusterNode currentLeader = get(node.get(ElectionService.class).leader(GROUP));

            return currentLeader.equals(expectedLeader);
        });
    }

    private CandidateMock getCandidate(HekateTestInstance node) {
        return node.getAttribute(CandidateMock.class.getName());
    }

    private HekateTestInstance createInstanceWithElection() throws Exception {
        return createInstanceWithElection(new CandidateMock());
    }

    private HekateTestInstance createInstanceWithElection(Candidate candidate) throws Exception {
        HekateTestInstance node = createInstance(c ->
            c.withService(new ElectionServiceFactory()
                .withCandidate(new CandidateConfig()
                    .withGroup(GROUP)
                    .withCandidate(candidate)
                )
            )
        );

        node.setAttribute(CandidateMock.class.getName(), candidate);

        return node;
    }
}
