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

package io.hekate.core.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.TerminateFuture;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.internal.netty.NettyNetworkService;
import io.hekate.network.internal.netty.NetworkServiceManagerMock;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.UP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HekateNodeTest extends HekateNodeTestBase {
    private HekateTestNode node;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createNode();
    }

    @Test
    public void testAttributes() throws Exception {
        assertNull(node.getAttribute("test"));

        node.setAttribute("test", "A");

        assertEquals("A", node.getAttribute("test"));

        node.join();

        assertEquals("A", node.getAttribute("test"));

        node.setAttribute("test", "B");

        assertEquals("B", node.getAttribute("test"));
        assertEquals("B", node.setAttribute("test", "C"));
        assertEquals("C", node.getAttribute("test"));

        node.setAttribute("test", null);

        assertNull(node.getAttribute("test"));

        node.setAttribute("test", "A");

        node.leave();

        assertEquals("A", node.getAttribute("test"));
    }

    @Test
    public void testJoinLeave() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::getLocalNode);
                expect(IllegalStateException.class, node::getTopology);
            } else {
                assertNotNull(node.getLocalNode());
                assertNotNull(node.getTopology());
            }

            assertSame(Hekate.State.DOWN, node.getState());

            node.join();

            assertNotNull(node.getLocalNode());
            assertSame(Hekate.State.UP, node.getState());
            assertNotNull(node.getTopology());
            assertEquals(1, node.getTopology().getNodes().size());
            assertTrue(node.getTopology().contains(node.getLocalNode()));

            node.leave();
        });
    }

    @Test
    public void testJoinTerminate() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::getLocalNode);
                expect(IllegalStateException.class, node::getTopology);
            } else {
                assertNotNull(node.getLocalNode());
                assertNotNull(node.getTopology());
            }

            assertSame(Hekate.State.DOWN, node.getState());

            node.join();

            assertNotNull(node.getLocalNode());
            assertEquals(1, node.getLocalNode().getJoinOrder());
            assertSame(Hekate.State.UP, node.getState());
            assertNotNull(node.getTopology());
            assertEquals(1, node.getTopology().getNodes().size());
            assertTrue(node.getTopology().contains(node.getLocalNode()));

            node.terminate();
        });
    }

    @Test
    public void testJoinFailureWhileLeaving() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::getLocalNode);
                expect(IllegalStateException.class, node::getTopology);
            } else {
                assertNotNull(node.getLocalNode());
                assertNotNull(node.getTopology());
            }

            assertSame(Hekate.State.DOWN, node.getState());

            node.join();

            LeaveFuture leave;

            node.getClusterGuard().lockWrite();

            try {
                leave = node.leaveAsync();

                expect(IllegalStateException.class, node::joinAsync);
            } finally {
                node.getClusterGuard().unlockWrite();
            }

            assertNotNull(get(leave));
        });
    }

    @Test
    public void testJoinLeaveNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Join " + i);

            JoinFuture join = node.joinAsync();

            say("Leave " + i);

            LeaveFuture leave = node.leaveAsync();

            join.get();
            leave.get();

            assertSame(Hekate.State.DOWN, node.getState());
        }
    }

    @Test
    public void testJoinTerminateNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Join " + i);

            JoinFuture join = node.joinAsync();

            say("Terminate " + i);

            TerminateFuture terminate = node.terminateAsync();

            join.get();
            terminate.get();

            assertSame(Hekate.State.DOWN, node.getState());
        }
    }

    @Test
    public void testMultipleJoinCalls() throws Exception {
        List<JoinFuture> futures = new ArrayList<>();

        List<ClusterEvent> events = new CopyOnWriteArrayList<>();

        ClusterEventListener listener = events::add;

        node.cluster().addListener(listener);

        for (int i = 0; i < 50; i++) {
            futures.add(node.joinAsync());
        }

        for (JoinFuture future : futures) {
            future.get();
        }

        node.cluster().removeListener(listener);

        node.leave();

        assertEquals(1, events.size());
        assertSame(ClusterEventType.JOIN, events.get(0).getType());
        assertEquals(1, events.get(0).getTopology().getLocalNode().getJoinOrder());
    }

    @Test
    public void testMultipleLeaveCalls() throws Exception {
        node.join();

        List<ClusterEvent> events = new CopyOnWriteArrayList<>();

        node.cluster().addListener(events::add);

        List<LeaveFuture> futures = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            futures.add(node.leaveAsync());
        }

        for (LeaveFuture future : futures) {
            future.get();
        }

        assertEquals(2, events.size());
        assertSame(ClusterEventType.JOIN, events.get(0).getType());
        assertSame(ClusterEventType.LEAVE, events.get(1).getType());
    }

    @Test
    public void testNetworkServiceStartupFailure() throws Exception {
        node = createNode(c -> c.findOrRegister(NetworkServiceFactory.class).setPortRange(0));

        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(node.getSocketAddress());

            repeat(10, i -> {
                try {
                    node.join();

                    fail();
                } catch (HekateFutureException e) {
                    HekateException cause = e.findCause(HekateException.class);

                    assertNotNull(cause.toString(), cause.getCause());
                    assertTrue(cause.getCause().toString(), cause.getCause() instanceof IOException);
                    assertTrue(cause.getMessage().contains("Address already in use"));

                    say(cause);
                }
            });
        }
    }

    @Test
    public void testNetworkServiceFailure() throws Exception {
        node = createNode(c -> {
            NetworkServiceFactory net = c.find(NetworkServiceFactory.class).get();

            c.getServices().remove(net);

            c.withService(() -> new NetworkServiceManagerMock((NettyNetworkService)net.createService()));
        });

        repeat(10, i -> {
            node.join();

            assertSame(Hekate.State.UP, node.getState());

            NetworkServiceManagerMock netMock = node.get(NetworkServiceManagerMock.class);

            netMock.fireServerFailure(new IOException(TEST_ERROR_MESSAGE));

            node.awaitForStatus(Hekate.State.DOWN);
        });
    }

    @Test
    public void testJoinLeaveFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = node.joinAsync();

            get(joinFuture.thenAccept(joined -> {
                assertNotNull(joined);

                assertTrue(joinFuture.isSuccess());
                assertTrue(joinFuture.isDone());

                assertNotNull(joined.getLocalNode());
                assertSame(UP, joined.getState());

                ClusterTopology topology = joined.cluster().getTopology();

                assertNotNull(topology);
                assertEquals(1, topology.getNodes().size());
                assertTrue(topology.contains(joined.getLocalNode()));
            }));

            LeaveFuture leaveFuture = node.leaveAsync();

            get(leaveFuture.thenAccept(left ->
                assertSame(DOWN, left.getState())
            ));
        });
    }

    @Test
    public void testJoinTerminateFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = node.joinAsync();

            get(joinFuture.thenAccept(joined -> {
                assertNotNull(joined);

                assertTrue(joinFuture.isSuccess());
                assertTrue(joinFuture.isDone());

                assertNotNull(joined.getLocalNode());
                assertSame(UP, joined.getState());

                ClusterTopology topology = joined.cluster().getTopology();

                assertNotNull(topology);
                assertEquals(1, topology.getNodes().size());
                assertTrue(topology.contains(joined.getLocalNode()));
            }));

            TerminateFuture terminateFuture = node.terminateAsync();

            get(terminateFuture.thenAccept(left ->
                assertSame(DOWN, left.getState())
            ));
        });
    }

    @Test
    public void testLeaveAsyncFromJoinFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = node.joinAsync();

            get(get(joinFuture.thenApply(joined -> {
                try {
                    return joined.leaveAsync();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            })));

            assertSame(Hekate.State.DOWN, joinFuture.get().getState());
        });
    }

    @Test
    public void testJoinAsyncFromLeaveFuture() throws Exception {
        repeat(5, i -> {
            node.join();

            LeaveFuture leaveFuture = node.leaveAsync();

            get(get(leaveFuture.thenApply(Hekate::joinAsync)));

            assertSame(Hekate.State.UP, leaveFuture.get().getState());

            node.leave();
        });
    }

    @Test
    public void testJoinAsyncFromTerminateFuture() throws Exception {
        repeat(5, i -> {
            node.join();

            TerminateFuture terminateFuture = node.terminateAsync();

            get(get(terminateFuture.thenApply(Hekate::joinAsync)));

            assertSame(Hekate.State.UP, terminateFuture.get().getState());

            node.leave();
        });
    }
}
