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

package io.hekate.cluster.internal;

import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterLeaveReason;
import io.hekate.cluster.internal.gossip.GossipNodeStatus;
import io.hekate.cluster.internal.gossip.GossipSpyAdaptor;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.core.LeaveFuture;
import io.hekate.core.TerminateFuture;
import io.hekate.core.internal.HekateTestNode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.INITIALIZED;
import static io.hekate.core.Hekate.State.INITIALIZING;
import static io.hekate.core.Hekate.State.LEAVING;
import static io.hekate.core.Hekate.State.TERMINATING;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ClusterServiceRejoinTest extends ClusterServiceMultipleNodesTestBase {
    public ClusterServiceRejoinTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testRejoinAfterHanged() throws Exception {
        disableNodeFailurePostCheck();

        List<HekateTestNode> nodes = createNodes(3);

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            awaitForTopology(nodes);

            for (HekateTestNode hanged : nodes) {
                ClusterNode hangedNode = hanged.localNode();

                AtomicInteger suspectCount = new AtomicInteger();

                CountDownLatch failureLatch = new CountDownLatch(1);

                nodes.forEach(n -> n.setGossipSpy(new GossipSpyAdaptor() {
                    @Override
                    public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
                        if (hangedNode.equals(failed)) {
                            failureLatch.countDown();
                        }
                    }

                    @Override
                    public void onNodeFailureSuspected(ClusterNode failed, GossipNodeStatus status) {
                        suspectCount.incrementAndGet();
                    }
                }));

                hanged.startRecording();

                AtomicBoolean inconsistencyDetected = new AtomicBoolean();

                hanged.setGossipSpy(new GossipSpyAdaptor() {
                    @Override
                    public void onNodeInconsistency(GossipNodeStatus status) {
                        inconsistencyDetected.set(true);
                    }
                });

                ClusterNodeId oldHangedId = hanged.localNode().id();

                hanged.clusterGuard().lockWrite();

                try {
                    say("Locked: " + hangedNode);

                    await(failureLatch);
                } finally {
                    hanged.clusterGuard().unlockWrite();
                }

                say("Unlocked: " + hangedNode);

                awaitForNodeChange(oldHangedId, hanged);

                assertTrue(inconsistencyDetected.get());

                // Default value of failure detection quorum.
                assertEquals(2, suspectCount.get());

                awaitForTopology(nodes);

                List<ClusterEvent> events = hanged.events();

                assertEquals(events.toString(), 2, events.size());
                assertSame(ClusterEventType.JOIN, events.get(0).type());
                assertSame(ClusterEventType.LEAVE, events.get(1).type());
                assertSame(ClusterLeaveReason.SPLIT_BRAIN, events.get(1).asLeave().reason());

                hanged.stopRecording();
                hanged.clearEvents();
            }

            for (HekateTestNode node : nodes) {
                node.leave();
            }
        });
    }

    @Test
    public void testNoRejoinAfterSuspectRecovery() throws Exception {
        List<HekateTestNode> nodes = createNodes(3);

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            awaitForTopology(nodes);

            for (HekateTestNode hanged : nodes) {
                AtomicBoolean onlyOneShouldBlock = new AtomicBoolean();
                CountDownLatch bothSuspected = new CountDownLatch(2);
                CountDownLatch firstUnsuspected = new CountDownLatch(1);
                CountDownLatch bothUnsuspected = new CountDownLatch(2);
                AtomicReference<Throwable> unexpectedError = new AtomicReference<>();

                ClusterNode hangedNode = hanged.localNode();

                nodes.forEach(n -> n.setGossipSpy(new GossipSpyAdaptor() {
                    @Override
                    public void onNodeFailureSuspected(ClusterNode failed, GossipNodeStatus status) {
                        if (failed.equals(hangedNode)) {
                            try {
                                bothSuspected.countDown();

                                await(bothSuspected);

                                if (!onlyOneShouldBlock.compareAndSet(false, true)) {
                                    await(firstUnsuspected);
                                }
                            } catch (Throwable e) {
                                unexpectedError.compareAndSet(null, e);
                            }
                        }
                    }

                    @Override
                    public void onNodeFailureUnsuspected(ClusterNode node, GossipNodeStatus status) {
                        if (node.equals(hangedNode)) {
                            firstUnsuspected.countDown();

                            bothUnsuspected.countDown();
                        }
                    }
                }));

                hanged.startRecording();

                hanged.clusterGuard().lockWrite();

                try {
                    say("Locked: " + hangedNode);

                    await(bothSuspected);
                } finally {
                    hanged.clusterGuard().unlockWrite();
                }

                say("Unlocked: " + hangedNode);

                await(bothUnsuspected);

                awaitForTopology(nodes);

                assertNull(unexpectedError.get());

                List<ClusterEvent> events = hanged.events();

                assertEquals(events.toString(), 1, events.size());
                assertSame(ClusterEventType.JOIN, events.get(0).type());

                hanged.stopRecording();
                hanged.clearEvents();
            }

            for (HekateTestNode node : nodes) {
                node.leave();
            }
        });
    }

    @Test
    public void testTerminateDuringRejoin() throws Exception {
        disableNodeFailurePostCheck();

        HekateTestNode alwaysAlive = createNode();

        alwaysAlive.join();

        List<HekateTestNode> nodes = createNodes(3);

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            List<HekateTestNode> alive = new ArrayList<>(nodes);

            alive.add(alwaysAlive);

            alive.forEach(n -> n.awaitForTopology(alive));

            for (HekateTestNode hanged : nodes) {
                alive.remove(hanged);

                hanged.startRecording();

                ClusterNode node = hanged.localNode();

                CountDownLatch rejoinLatch = new CountDownLatch(1);
                CountDownLatch rejoinContinueLatch = new CountDownLatch(1);
                AtomicReference<Throwable> asyncError = new AtomicReference<>();

                hanged.cluster().addListener(e -> {
                    if (e.type() == ClusterEventType.LEAVE) {
                        rejoinLatch.countDown();

                        try {
                            await(rejoinContinueLatch);
                        } catch (Throwable err) {
                            asyncError.set(err);
                        }
                    }
                });

                hanged.clusterGuard().lockWrite();

                try {
                    say("Locked: " + node);

                    alive.forEach(n -> n.awaitForTopology(alive));
                } finally {
                    hanged.clusterGuard().unlockWrite();
                }

                say("Unlocked: " + node);

                await(rejoinLatch);

                TerminateFuture terminateFuture = hanged.terminateAsync();

                rejoinContinueLatch.countDown();

                assertNotNull(get(terminateFuture));

                assertSame(DOWN, hanged.state());
                assertNull(asyncError.get());

                List<ClusterEvent> events = hanged.events();

                assertEquals(events.toString(), 2, events.size());
                assertSame(ClusterEventType.JOIN, events.get(0).type());
                assertSame(ClusterEventType.LEAVE, events.get(1).type());
                assertSame(ClusterLeaveReason.SPLIT_BRAIN, events.get(1).asLeave().reason());

                hanged.stopRecording();
                hanged.clearEvents();
            }
        });
    }

    @Test
    public void testLeaveDuringRejoin() throws Exception {
        disableNodeFailurePostCheck();

        HekateTestNode alwaysAlive = createNode();

        alwaysAlive.join();

        List<HekateTestNode> nodes = createNodes(3);

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            List<HekateTestNode> alive = new ArrayList<>(nodes);

            alive.add(alwaysAlive);

            alive.forEach(n -> n.awaitForTopology(alive));

            for (HekateTestNode hanged : nodes) {
                alive.remove(hanged);

                hanged.startRecording();

                ClusterNode node = hanged.localNode();

                CountDownLatch rejoinLatch = new CountDownLatch(1);
                CountDownLatch rejoinContinueLatch = new CountDownLatch(1);
                AtomicReference<Throwable> asyncError = new AtomicReference<>();

                hanged.cluster().addListener(e -> {
                    if (e.type() == ClusterEventType.LEAVE) {
                        rejoinLatch.countDown();

                        try {
                            await(rejoinContinueLatch);
                        } catch (Throwable err) {
                            asyncError.set(err);
                        }
                    }
                });

                hanged.clusterGuard().lockWrite();

                try {
                    say("Locked: " + node);

                    alive.forEach(n -> n.awaitForTopology(alive));
                } finally {
                    hanged.clusterGuard().unlockWrite();
                }

                say("Unlocked: " + node);

                await(rejoinLatch);

                LeaveFuture leaveFuture = hanged.leaveAsync();

                rejoinContinueLatch.countDown();

                assertNotNull(get(leaveFuture));

                assertSame(DOWN, hanged.state());
                assertNull(asyncError.get());

                List<ClusterEvent> events = hanged.events();

                assertEquals(events.toString(), 2, events.size());
                assertSame(ClusterEventType.JOIN, events.get(0).type());
                assertSame(ClusterEventType.LEAVE, events.get(1).type());
                assertSame(ClusterLeaveReason.SPLIT_BRAIN, events.get(1).asLeave().reason());

                hanged.stopRecording();
                hanged.clearEvents();
            }
        });
    }

    @Test
    public void testNoRejoinOnLeave() throws Exception {
        disableNodeFailurePostCheck();

        HekateTestNode alwaysAlive = createNode();

        alwaysAlive.join();

        List<HekateTestNode> nodes = createNodes(3);

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            List<HekateTestNode> alive = new ArrayList<>(nodes);

            alive.add(alwaysAlive);

            alive.forEach(n -> n.awaitForTopology(alive));

            for (HekateTestNode hanged : nodes) {
                alive.remove(hanged);

                LeaveFuture leave;

                hanged.startRecording();

                hanged.clusterGuard().lockWrite();

                try {
                    say("Locked: " + hanged);

                    leave = hanged.leaveAsync();

                    assertSame(LEAVING, hanged.state());

                    alive.forEach(n -> n.awaitForTopology(alive));
                } finally {
                    hanged.clusterGuard().unlockWrite();
                }

                say("Unlocked: " + hanged);

                assertNotNull(get(leave));

                assertSame(DOWN, hanged.state());

                List<ClusterEvent> events = hanged.events();

                assertEquals(events.toString(), 2, events.size());
                assertSame(ClusterEventType.JOIN, events.get(0).type());
                assertSame(ClusterEventType.LEAVE, events.get(1).type());
                assertSame(ClusterLeaveReason.LEAVE, events.get(1).asLeave().reason());

                hanged.stopRecording();
                hanged.clearEvents();
            }
        });
    }

    @Test
    public void testTerminateAfterFalseFailure() throws Exception {
        disableNodeFailurePostCheck();

        List<HekateTestNode> nodes = createNodes(3, c ->
            c.service(ClusterServiceFactory.class).get().setSplitBrainAction(SplitBrainAction.TERMINATE)
        );

        repeat(3, i -> {
            for (HekateTestNode node : nodes) {
                node.join();
            }

            awaitForTopology(nodes);

            HekateTestNode hanged = nodes.get(i);
            CountDownLatch failureDetected = new CountDownLatch(1);

            ClusterNode hangedNode = hanged.localNode();

            List<HekateTestNode> nodesWithoutHanged = nodes.stream().filter(n -> !n.equals(hanged)).collect(toList());

            nodes.forEach(n -> n.setGossipSpy(new GossipSpyAdaptor() {
                @Override
                public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
                    if (hangedNode.equals(failed)) {
                        failureDetected.countDown();
                    }
                }
            }));

            hanged.startRecording();

            AtomicBoolean inconsistencyDetected = new AtomicBoolean();

            hanged.setGossipSpy(new GossipSpyAdaptor() {
                @Override
                public void onNodeInconsistency(GossipNodeStatus status) {
                    inconsistencyDetected.set(true);
                }
            });

            hanged.clusterGuard().lockWrite();

            try {
                say("Locked: " + hangedNode);

                await(failureDetected);
            } finally {
                hanged.clusterGuard().unlockWrite();
            }

            say("Unlocked: " + hangedNode);

            awaitForTopology(nodesWithoutHanged);

            hanged.awaitForStatus(DOWN);

            assertTrue(inconsistencyDetected.get());

            List<ClusterEvent> events = hanged.events();

            assertEquals(events.toString(), 2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).type());
            assertSame(ClusterEventType.LEAVE, events.get(1).type());
            assertSame(ClusterLeaveReason.SPLIT_BRAIN, events.get(1).asLeave().reason());

            hanged.stopRecording();
            hanged.clearEvents();

            for (HekateTestNode node : nodes) {
                node.leave();
            }
        });
    }

    private void awaitForNodeChange(ClusterNodeId id, HekateTestNode node) throws Exception {
        busyWait("node change from " + id, () -> {
            assertTrue(node.clusterGuard().tryLockRead(AWAIT_TIMEOUT, TimeUnit.SECONDS));

            try {
                return node.state() != DOWN
                    && node.state() != INITIALIZING
                    && node.state() != INITIALIZED
                    && node.state() != TERMINATING
                    && !node.localNode().id().equals(id);
            } finally {
                node.clusterGuard().unlockRead();
            }
        });
    }
}
