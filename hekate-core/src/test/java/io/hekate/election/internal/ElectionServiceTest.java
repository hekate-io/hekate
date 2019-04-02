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

package io.hekate.election.internal;

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.election.Candidate;
import io.hekate.election.CandidateConfig;
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

public class ElectionServiceTest extends HekateNodeParamTestBase {
    private static final String GROUP = "GROUP";

    public ElectionServiceTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testConcurrentElectionWithLeave() throws Exception {
        doTestConcurrentElection(node -> {
            say("Leave " + node.localNode());

            try {
                get(node.leaveAsync());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    @Test
    public void testConcurrentElectionWithTerminate() throws Exception {
        disableNodeFailurePostCheck();

        doTestConcurrentElection(node -> {
            say("Terminate " + node.localNode());

            try {
                get(node.terminateAsync());
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        });
    }

    @Test
    public void testYieldLeadershipSingleNode() throws Exception {
        HekateTestNode node = createElectionNode().join();

        CandidateMock candidate = candidateOf(node);

        for (int i = 0; i < 3; i++) {
            assertEquals(node.localNode(), get(node.election().leader(GROUP)));

            candidate.yieldLeadership();

            candidate.awaitForBecomeLeader();

            assertEquals(node.localNode(), get(node.election().leader(GROUP)));
        }
    }

    @Test
    public void testYieldLeadershipSameNodes() throws Exception {
        // Start leader.
        HekateTestNode node1 = createElectionNode().join();

        get(node1.election().leader(GROUP));

        // Start follower.
        HekateTestNode node2 = createElectionNode().join();

        for (int i = 0; i < 5; i++) {
            say("Iteration: " + i);

            // Make sure that node1 is the leader.
            assertEquals(node1.localNode(), get(node1.election().leader(GROUP)));
            assertEquals(node1.localNode(), get(node2.election().leader(GROUP)));

            CandidateMock candidate1 = candidateOf(node1);
            CandidateMock candidate2 = candidateOf(node2);

            // Yield leadership.
            candidate1.yieldLeadership();

            // Await for leadership change.
            candidate1.awaitForLeaderChange(node1.localNode());
            candidate2.awaitForLeaderChange(node1.localNode());

            // Verify that node2 became the leader.
            assertTrue(candidate1.isFollower());
            assertTrue(candidate2.isLeader());

            awaitForLeaderFuture(node2.localNode(), node1);
            awaitForLeaderFuture(node2.localNode(), node2);

            // Invert nodes for the next iteration.
            HekateTestNode temp = node1;

            node1 = node2;
            node2 = temp;
        }
    }

    @Test
    public void testYieldLeadershipNewNodes() throws Exception {
        repeat(5, i -> {
            // Start leader.
            HekateTestNode node1 = createElectionNode().join();

            get(node1.election().leader(GROUP));

            // Start follower.
            HekateTestNode node2 = createElectionNode().join();

            // Make sure that node1 is the leader.
            assertEquals(node1.localNode(), get(node1.election().leader(GROUP)));
            assertEquals(node1.localNode(), get(node2.election().leader(GROUP)));

            CandidateMock candidate1 = candidateOf(node1);
            CandidateMock candidate2 = candidateOf(node2);

            // Yield leadership.
            candidate1.yieldLeadership();

            // Await for leadership change.
            candidate1.awaitForLeaderChange(node1.localNode());
            candidate2.awaitForLeaderChange(node1.localNode());

            // Verify that node2 became the leader.
            assertTrue(candidate1.isFollower());
            assertTrue(candidate2.isLeader());

            awaitForLeaderFuture(node2.localNode(), node1);
            awaitForLeaderFuture(node2.localNode(), node2);

            node1.leave();
            node2.leave();
        });
    }

    @Test
    public void testLeaderLeave() throws Exception {
        // Start leader.
        HekateTestNode leader = createElectionNode().join();

        get(leader.election().leader(GROUP));

        for (int i = 0; i < 5; i++) {
            say("Iteration: " + i);

            // Start follower.
            HekateTestNode follower = createElectionNode().join();

            // Verify leader.
            assertEquals(leader.localNode(), get(follower.election().leader(GROUP)));

            CandidateMock followerCandidate = candidateOf(follower);

            ClusterNode oldLeader = leader.localNode();

            // Leader leaves the cluster.
            leader.leave();

            // Await for leadership change.
            followerCandidate.awaitForLeaderChange(oldLeader);

            // Verify that follower became the new leader.
            assertTrue(followerCandidate.isLeader());

            awaitForLeaderFuture(follower.localNode(), follower);

            // Old follower becomes a new leader of the next iteration.
            leader = follower;
        }
    }

    @Test
    public void testClusterChange() throws Exception {
        List<HekateTestNode> nodes = new ArrayList<>();

        repeat(5, i -> {
            // Start few more nodes.
            nodes.add(createElectionNode().join());
            nodes.add(createElectionNode().join());

            busyWait("Same leader", () -> {
                Set<ClusterNode> leaders = new HashSet<>();

                for (HekateTestNode node : nodes) {
                    leaders.add(get(node.election().leader(GROUP)));
                }

                return leaders.size() == 1;
            });

            Set<ClusterNode> leaders = new HashSet<>();

            for (HekateTestNode node : nodes) {
                ClusterNode leader = get(node.election().leader(GROUP));

                leaders.add(leader);

                assertTrue(nodes.stream().anyMatch(n -> n.localNode().equals(leader)));

                CandidateMock candidate = candidateOf(node);

                if (leader.equals(node.localNode())) {
                    assertTrue(candidate.isLeader());
                } else {
                    assertTrue(candidate.isFollower());
                    assertEquals(leader, candidate.lastLeader());
                }
            }

            assertEquals(leaders.toString(), 1, leaders.size());

            // Stop leader.
            HekateTestNode oldLeader = nodes.stream()
                .filter(n -> leaders.contains(n.localNode()))
                .findFirst()
                .orElseThrow(AssertionError::new);

            ClusterNode oldLeaderNode = oldLeader.localNode();

            nodes.remove(oldLeader);

            CandidateMock oldLeaderCandidate = candidateOf(oldLeader);

            say("Stopping leader " + oldLeader.localNode());

            oldLeader.leave();

            oldLeaderCandidate.awaitTermination();

            for (HekateTestNode node : nodes) {
                say("Checking " + node.localNode());

                candidateOf(node).awaitForLeaderChange(oldLeaderNode);
            }
        });
    }

    private void doTestConcurrentElection(Consumer<HekateTestNode> nodeStopAction) throws Exception {
        repeat(5, i -> {
            AtomicInteger leaderIdx = new AtomicInteger();
            AtomicReference<AssertionError> err = new AtomicReference<>();

            List<HekateTestNode> nodes = new ArrayList<>();

            for (int j = 0; j < 2; j++) {
                int idx = j + 1;

                nodes.add(createElectionNode(new Candidate() {
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

    private void awaitForLeaderFuture(ClusterNode expectedLeader, HekateTestNode node) throws Exception {
        busyWait("leader " + expectedLeader, () -> {
            ClusterNode currentLeader = get(node.election().leader(GROUP));

            return currentLeader.equals(expectedLeader);
        });
    }

    private CandidateMock candidateOf(HekateTestNode node) {
        return (CandidateMock)node.getAttribute(CandidateMock.class.getName());
    }

    private HekateTestNode createElectionNode() throws Exception {
        return createElectionNode(new CandidateMock());
    }

    private HekateTestNode createElectionNode(Candidate candidate) throws Exception {
        HekateTestNode node = createNode(c ->
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
