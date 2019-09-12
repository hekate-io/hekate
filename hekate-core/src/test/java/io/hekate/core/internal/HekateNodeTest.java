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

package io.hekate.core.internal;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.InitializationFuture;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.TerminateFuture;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.Service;
import io.hekate.core.service.TerminatingService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.internal.NettyNetworkService;
import io.hekate.network.internal.NetworkServiceManagerMock;
import io.hekate.test.HekateTestError;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.UP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class HekateNodeTest extends HekateNodeTestBase {
    private interface TestService extends Service, InitializingService, TerminatingService {
        // No-op.
    }

    private HekateTestNode node;

    private TestService serviceMock;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        serviceMock = mock(TestService.class);

        node = createNode(boot ->
            boot.withService(() -> serviceMock)
        );
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
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.join();

            assertNotNull(node.localNode());
            assertSame(Hekate.State.UP, node.state());
            assertNotNull(node.topology());
            assertEquals(1, node.topology().nodes().size());
            assertTrue(node.topology().contains(node.localNode()));

            node.leave();
        });
    }

    @Test
    public void testInitializeJoinLeave() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.initialize();

            ClusterNode localNode = node.localNode();

            assertNotNull(localNode);
            assertEquals(0, localNode.joinOrder());
            assertSame(Hekate.State.INITIALIZED, node.state());
            assertNotNull(node.topology());
            assertTrue(node.topology().isEmpty());

            node.join();

            assertNotNull(node.localNode());
            assertSame(Hekate.State.UP, node.state());
            assertEquals(1, localNode.joinOrder());
            assertNotNull(node.topology());
            assertEquals(1, node.topology().nodes().size());
            assertTrue(node.topology().contains(node.localNode()));

            node.leave();
        });
    }

    @Test
    public void testInitializeLeave() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.initialize();

            assertNotNull(node.localNode());
            assertSame(Hekate.State.INITIALIZED, node.state());
            assertNotNull(node.topology());
            assertTrue(node.topology().isEmpty());

            node.leave();
        });
    }

    @Test
    public void testJoinTerminate() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.join();

            assertNotNull(node.localNode());
            assertEquals(1, node.localNode().joinOrder());
            assertSame(Hekate.State.UP, node.state());
            assertNotNull(node.topology());
            assertEquals(1, node.topology().nodes().size());
            assertTrue(node.topology().contains(node.localNode()));

            node.terminate();
        });
    }

    @Test
    public void testInitializeTerminate() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.initialize();

            assertNotNull(node.localNode());
            assertEquals(0, node.localNode().joinOrder());
            assertSame(Hekate.State.INITIALIZED, node.state());
            assertNotNull(node.topology());
            assertTrue(node.topology().isEmpty());

            node.terminate();
        });
    }

    @Test
    public void testJoinFailureWhileLeaving() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, node::localNode);
                expect(IllegalStateException.class, node::topology);
            } else {
                assertNotNull(node.localNode());
                assertNotNull(node.topology());
            }

            assertSame(Hekate.State.DOWN, node.state());

            node.join();

            LeaveFuture leave;

            node.clusterGuard().lockWrite();

            try {
                leave = node.leaveAsync();

                expect(IllegalStateException.class, node::joinAsync);
            } finally {
                node.clusterGuard().unlockWrite();
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

            get(join);
            get(leave);

            assertSame(Hekate.State.DOWN, node.state());
        }
    }

    @Test
    public void testJoinTerminateNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Join " + i);

            JoinFuture join = node.joinAsync();

            say("Terminate " + i);

            TerminateFuture terminate = node.terminateAsync();

            get(join);
            get(terminate);

            assertSame(Hekate.State.DOWN, node.state());
        }
    }

    @Test
    public void testInitializeTerminateNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Initialize " + i);

            InitializationFuture join = node.initializeAsync();

            say("Terminate " + i);

            TerminateFuture terminate = node.terminateAsync();

            get(join);
            get(terminate);

            assertSame(Hekate.State.DOWN, node.state());
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
            get(future);
        }

        node.cluster().removeListener(listener);

        node.leave();

        assertEquals(1, events.size());
        assertSame(ClusterEventType.JOIN, events.get(0).type());
        assertEquals(1, events.get(0).topology().localNode().joinOrder());
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
            get(future);
        }

        assertEquals(2, events.size());
        assertSame(ClusterEventType.JOIN, events.get(0).type());
        assertSame(ClusterEventType.LEAVE, events.get(1).type());
    }

    @Test
    public void testNetworkServiceStartupFailure() throws Exception {
        try (ServerSocket sock = new ServerSocket()) {
            InetSocketAddress address = newSocketAddress();

            sock.bind(address);

            node = createNode(boot ->
                boot.withService(NetworkServiceFactory.class, net -> {
                    net.setPort(address.getPort());
                    net.setPortRange(0);
                    net.setTcpReuseAddress(false);
                })
            );

            repeat(5, i -> {
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
            NetworkServiceFactory net = c.service(NetworkServiceFactory.class).get();

            c.getServices().remove(net);

            c.withService(() -> new NetworkServiceManagerMock((NettyNetworkService)net.createService()));
        });

        repeat(10, i -> {
            node.join();

            assertSame(Hekate.State.UP, node.state());

            NetworkServiceManagerMock netMock = node.get(NetworkServiceManagerMock.class);

            netMock.fireServerFailure(new IOException(HekateTestError.MESSAGE));

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

                assertNotNull(joined.localNode());
                assertSame(UP, joined.state());

                ClusterTopology topology = joined.cluster().topology();

                assertNotNull(topology);
                assertEquals(1, topology.nodes().size());
                assertTrue(topology.contains(joined.localNode()));
            }));

            LeaveFuture leaveFuture = node.leaveAsync();

            get(leaveFuture.thenAccept(left ->
                assertSame(DOWN, left.state())
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

                assertNotNull(joined.localNode());
                assertSame(UP, joined.state());

                ClusterTopology topology = joined.cluster().topology();

                assertNotNull(topology);
                assertEquals(1, topology.nodes().size());
                assertTrue(topology.contains(joined.localNode()));
            }));

            TerminateFuture terminateFuture = node.terminateAsync();

            get(terminateFuture.thenAccept(left ->
                assertSame(DOWN, left.state())
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

            assertSame(Hekate.State.DOWN, get(joinFuture).state());
        });
    }

    @Test
    public void testJoinAsyncFromLeaveFuture() throws Exception {
        repeat(5, i -> {
            node.join();

            LeaveFuture leaveFuture = node.leaveAsync();

            get(get(leaveFuture.thenApply(Hekate::joinAsync)));

            assertSame(Hekate.State.UP, get(leaveFuture).state());

            node.leave();
        });
    }

    @Test
    public void testJoinAsyncFromTerminateFuture() throws Exception {
        repeat(5, i -> {
            node.join();

            TerminateFuture terminateFuture = node.terminateAsync();

            get(get(terminateFuture.thenApply(Hekate::joinAsync)));

            assertSame(Hekate.State.UP, get(terminateFuture).state());

            node.leave();
        });
    }

    @Test
    public void testServiceSyncFuture() throws Exception {
        CompletableFuture<?> syncFuture = new CompletableFuture<>();

        doAnswer(invocationOnMock -> {
            InitializationContext ctx = invocationOnMock.getArgument(0);

            ctx.cluster().addSyncFuture(syncFuture);

            return null;
        }).when(serviceMock).initialize(any(InitializationContext.class));

        JoinFuture join = node.joinAsync();

        node.awaitForStatus(Hekate.State.SYNCHRONIZING);

        assertFalse(join.isDone());

        syncFuture.complete(null);

        get(join);
    }
}
