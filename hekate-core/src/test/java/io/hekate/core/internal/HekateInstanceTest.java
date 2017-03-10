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

import io.hekate.HekateInstanceTestBase;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.HekateTestInstance;
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
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HekateInstanceTest extends HekateInstanceTestBase {
    private HekateTestInstance instance;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        instance = createInstance();
    }

    @Test
    public void testAttributes() throws Exception {
        assertNull(instance.getAttribute("test"));

        instance.setAttribute("test", "A");

        assertEquals("A", instance.getAttribute("test"));

        instance.join();

        assertEquals("A", instance.getAttribute("test"));

        instance.setAttribute("test", "B");

        assertEquals("B", instance.getAttribute("test"));
        assertEquals("B", instance.setAttribute("test", "C"));
        assertEquals("C", instance.getAttribute("test"));

        instance.setAttribute("test", null);

        assertNull(instance.getAttribute("test"));

        instance.setAttribute("test", "A");

        instance.leave();

        assertEquals("A", instance.getAttribute("test"));
    }

    @Test
    public void testJoinLeave() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, instance::getNode);
                expect(IllegalStateException.class, instance::getTopology);
            } else {
                assertNotNull(instance.getNode());
                assertNotNull(instance.getTopology());
            }

            assertSame(Hekate.State.DOWN, instance.getState());

            instance.join();

            assertNotNull(instance.getNode());
            assertSame(Hekate.State.UP, instance.getState());
            assertNotNull(instance.getTopology());
            assertEquals(1, instance.getTopology().getNodes().size());
            assertTrue(instance.getTopology().contains(instance.getNode()));

            instance.leave();
        });
    }

    @Test
    public void testJoinTerminate() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, instance::getNode);
                expect(IllegalStateException.class, instance::getTopology);
            } else {
                assertNotNull(instance.getNode());
                assertNotNull(instance.getTopology());
            }

            assertSame(Hekate.State.DOWN, instance.getState());

            instance.join();

            assertNotNull(instance.getNode());
            assertEquals(1, instance.getNode().getJoinOrder());
            assertSame(Hekate.State.UP, instance.getState());
            assertNotNull(instance.getTopology());
            assertEquals(1, instance.getTopology().getNodes().size());
            assertTrue(instance.getTopology().contains(instance.getNode()));

            instance.terminate();
        });
    }

    @Test
    public void testJoinFailureWhileLeaving() throws Exception {
        repeat(50, i -> {
            if (i == 0) {
                expect(IllegalStateException.class, instance::getNode);
                expect(IllegalStateException.class, instance::getTopology);
            } else {
                assertNotNull(instance.getNode());
                assertNotNull(instance.getTopology());
            }

            assertSame(Hekate.State.DOWN, instance.getState());

            instance.join();

            LeaveFuture leave;

            instance.getClusterGuard().lockWrite();

            try {
                leave = instance.leaveAsync();

                expect(IllegalStateException.class, instance::joinAsync);
            } finally {
                instance.getClusterGuard().unlockWrite();
            }

            assertNotNull(leave.get(3, TimeUnit.SECONDS));
        });
    }

    @Test
    public void testJoinLeaveNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Join " + i);

            JoinFuture join = instance.joinAsync();

            say("Leave " + i);

            LeaveFuture leave = instance.leaveAsync();

            join.get();
            leave.get();

            assertSame(Hekate.State.DOWN, instance.getState());
        }
    }

    @Test
    public void testJoinTerminateNoWait() throws Exception {
        for (int i = 0; i < 50; i++) {
            say("Join " + i);

            JoinFuture join = instance.joinAsync();

            say("Terminate " + i);

            TerminateFuture terminate = instance.terminateAsync();

            join.get();
            terminate.get();

            assertSame(Hekate.State.DOWN, instance.getState());
        }
    }

    @Test
    public void testMultipleJoinCalls() throws Exception {
        List<JoinFuture> futures = new ArrayList<>();

        List<ClusterEvent> events = new CopyOnWriteArrayList<>();

        ClusterEventListener listener = events::add;

        instance.get(ClusterService.class).addListener(listener);

        for (int i = 0; i < 50; i++) {
            futures.add(instance.joinAsync());
        }

        for (JoinFuture future : futures) {
            future.get();
        }

        instance.get(ClusterService.class).removeListener(listener);

        instance.leave();

        assertEquals(1, events.size());
        assertSame(ClusterEventType.JOIN, events.get(0).getType());
        assertEquals(1, events.get(0).getTopology().getLocalNode().getJoinOrder());
    }

    @Test
    public void testMultipleLeaveCalls() throws Exception {
        instance.join();

        List<ClusterEvent> events = new CopyOnWriteArrayList<>();

        instance.get(ClusterService.class).addListener(events::add);

        List<LeaveFuture> futures = new ArrayList<>();

        for (int i = 0; i < 50; i++) {
            futures.add(instance.leaveAsync());
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
        instance = createInstance(c -> c.findOrRegister(NetworkServiceFactory.class).setPortRange(0));

        try (ServerSocket sock = new ServerSocket()) {
            sock.bind(instance.getSocketAddress());

            repeat(10, i -> {
                try {
                    instance.join();

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
        instance = createInstance(c -> {
            NetworkServiceFactory net = c.find(NetworkServiceFactory.class).get();

            c.getServices().remove(net);

            c.withService(() -> new NetworkServiceManagerMock((NettyNetworkService)net.createService()));
        });

        repeat(10, i -> {
            instance.join();

            assertSame(Hekate.State.UP, instance.getState());

            NetworkServiceManagerMock netMock = instance.get(NetworkServiceManagerMock.class);

            netMock.fireServerFailure(new IOException(TEST_ERROR_MESSAGE));

            instance.awaitForStatus(Hekate.State.DOWN);
        });
    }

    @Test
    public void testJoinLeaveFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = instance.joinAsync();

            joinFuture.thenAccept(joined -> {
                assertNotNull(joined);

                assertTrue(joinFuture.isSuccess());
                assertTrue(joinFuture.isDone());

                assertNotNull(joined.getNode());
                assertSame(Hekate.State.UP, joined.getState());

                ClusterTopology topology = joined.get(ClusterService.class).getTopology();

                assertNotNull(topology);
                assertEquals(1, topology.getNodes().size());
                assertTrue(topology.contains(joined.getNode()));
            }).get(3, TimeUnit.SECONDS);

            LeaveFuture leaveFuture = instance.leaveAsync();

            leaveFuture.thenAccept(left ->
                assertSame(Hekate.State.DOWN, left.getState())
            ).get(3, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testJoinTerminateFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = instance.joinAsync();

            joinFuture.thenAccept(joined -> {
                assertNotNull(joined);

                assertTrue(joinFuture.isSuccess());
                assertTrue(joinFuture.isDone());

                assertNotNull(joined.getNode());
                assertSame(Hekate.State.UP, joined.getState());

                ClusterTopology topology = joined.get(ClusterService.class).getTopology();

                assertNotNull(topology);
                assertEquals(1, topology.getNodes().size());
                assertTrue(topology.contains(joined.getNode()));
            }).get(3, TimeUnit.SECONDS);

            TerminateFuture terminateFuture = instance.terminateAsync();

            terminateFuture.thenAccept(left ->
                assertSame(Hekate.State.DOWN, left.getState())
            ).get(3, TimeUnit.SECONDS);
        });
    }

    @Test
    public void testLeaveAsyncFromJoinFuture() throws Exception {
        repeat(5, i -> {
            JoinFuture joinFuture = instance.joinAsync();

            joinFuture.thenApply(joined -> {
                try {
                    return joined.leaveAsync();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).get(3, TimeUnit.SECONDS).get(3, TimeUnit.SECONDS);

            assertSame(Hekate.State.DOWN, joinFuture.get().getState());
        });
    }

    @Test
    public void testJoinAsyncFromLeaveFuture() throws Exception {
        repeat(5, i -> {
            instance.join();

            LeaveFuture leaveFuture = instance.leaveAsync();

            leaveFuture.thenApply(Hekate::joinAsync).get(3, TimeUnit.SECONDS).get(3, TimeUnit.SECONDS);

            assertSame(Hekate.State.UP, leaveFuture.get().getState());

            instance.leave();
        });
    }

    @Test
    public void testJoinAsyncFromTerminateFuture() throws Exception {
        repeat(5, i -> {
            instance.join();

            TerminateFuture terminateFuture = instance.terminateAsync();

            terminateFuture.thenApply(Hekate::joinAsync).get(3, TimeUnit.SECONDS).get(3, TimeUnit.SECONDS);

            assertSame(Hekate.State.UP, terminateFuture.get().getState());

            instance.leave();
        });
    }
}
