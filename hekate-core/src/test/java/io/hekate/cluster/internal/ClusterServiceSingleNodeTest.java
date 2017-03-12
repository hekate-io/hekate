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

package io.hekate.cluster.internal;

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.seed.SeedNodeProviderAdaptor;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.core.Hekate.State;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.HekateTestInstance;
import io.hekate.core.JoinFuture;
import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterServiceSingleNodeTest extends HekateInstanceContextTestBase {
    private HekateTestInstance instance;

    public ClusterServiceSingleNodeTest(HekateTestContext params) {
        super(params);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        instance = createInstance();
    }

    @Test
    public void testJoinLeaveEventsAfterJoin() throws Exception {
        repeat(50, i -> {
            instance.join();

            ClusterNode node = instance.getNode();

            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            instance.get(ClusterService.class).addListener(listener);

            instance.leave();

            instance.get(ClusterService.class).removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).getType());
            assertSame(ClusterEventType.LEAVE, events.get(1).getType());

            ClusterJoinEvent join = events.get(0).asJoin();
            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, join.getTopology().size());
            assertEquals(node, join.getTopology().getLocalNode());
            assertEquals(1, join.getTopology().getLocalNode().getJoinOrder());
            assertTrue(join.getTopology().contains(node));

            assertEquals(1, leave.getTopology().size());
            assertEquals(node, leave.getTopology().getLocalNode());
            assertEquals(1, leave.getTopology().getLocalNode().getJoinOrder());
            assertTrue(leave.getTopology().contains(node));
        });
    }

    @Test
    public void testJoinLeaveEventsWithTerminate() throws Exception {
        repeat(50, i -> {
            instance.join();

            ClusterNode node = instance.getNode();

            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            instance.get(ClusterService.class).addListener(listener);

            instance.terminate();

            instance.get(ClusterService.class).removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).getType());
            assertSame(ClusterEventType.LEAVE, events.get(1).getType());

            ClusterJoinEvent join = events.get(0).asJoin();
            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, join.getTopology().size());
            assertEquals(node, join.getTopology().getLocalNode());
            assertEquals(1, join.getTopology().getLocalNode().getJoinOrder());
            assertTrue(join.getTopology().contains(node));

            assertEquals(1, leave.getTopology().size());
            assertEquals(node, leave.getTopology().getLocalNode());
            assertEquals(1, leave.getTopology().getLocalNode().getJoinOrder());
            assertTrue(leave.getTopology().contains(node));
        });
    }

    @Test
    public void testJoinLeaveEventsBeforeJoin() throws Exception {
        repeat(50, i -> {
            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            instance.get(ClusterService.class).addListener(listener);

            instance.join();

            ClusterNode node = instance.getNode();

            instance.leave();

            instance.get(ClusterService.class).removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).getType());
            assertSame(ClusterEventType.LEAVE, events.get(1).getType());

            ClusterJoinEvent join = events.get(0).asJoin();
            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, join.getTopology().size());
            assertEquals(node, join.getTopology().getLocalNode());
            assertEquals(node, join.getTopology().getLocalNode());
            assertTrue(join.getTopology().contains(node));
            assertFalse(join.getTopology().getRemoteNodes().contains(node));

            assertEquals(1, leave.getTopology().size());
            assertEquals(node, leave.getTopology().getLocalNode());
            assertEquals(1, join.getTopology().getLocalNode().getJoinOrder());
            assertTrue(leave.getTopology().contains(node));
            assertFalse(leave.getTopology().getRemoteNodes().contains(node));
        });
    }

    @Test
    public void testStateInListenerBeforeJoin() throws Exception {
        repeat(50, i -> {
            List<State> statuses = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = event -> statuses.add(instance.getState());

            instance.get(ClusterService.class).addListener(listener);

            instance.join();

            instance.leave();

            instance.get(ClusterService.class).removeListener(listener);

            assertEquals(2, statuses.size());
            assertSame(State.UP, statuses.get(0));
            assertSame(State.LEAVING, statuses.get(1));
        });
    }

    @Test
    public void testJoinWithInvalidSeedNode() throws Exception {
        InetAddress addr = InetAddress.getLocalHost();
        String host = addr instanceof Inet6Address ? '[' + addr.getHostAddress() + ']' : addr.getHostAddress();

        int port = newTcpPort();

        AtomicBoolean seedStopped = new AtomicBoolean();
        CountDownLatch seedBindLatch = new CountDownLatch(1);

        Future<Void> seedFuture = runAsync(() -> {
            try (ServerSocket server = new ServerSocket()) {
                server.bind(new InetSocketAddress(addr, port));

                seedBindLatch.countDown();

                while (!seedStopped.get()) {
                    // Immediately reject connection.
                    server.accept().close();
                }
            }

            return null;
        });

        await(seedBindLatch);

        try {
            // Register fake address.
            seedNodes.setDelegate(new StaticSeedNodeProvider(new StaticSeedNodeProviderConfig()
                .withAddress(host + ':' + port))
            );

            // Try join.
            assertSame(State.UP, instance.joinAsync().get(3, TimeUnit.SECONDS).getState());
        } finally {
            seedStopped.set(true);

            // Ping server to wake it up.
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(addr, port));
            } catch (IOException e) {
                // Ignore.
            }

            seedFuture.get(3, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testLeaveWhileRegisteringSeedNode() throws Exception {
        repeat(50, i -> {
            CountDownLatch hangLatch = new CountDownLatch(1);
            CountDownLatch resumeLatch = new CountDownLatch(1);

            AtomicReference<InetSocketAddress> registered = new AtomicReference<>();
            AtomicReference<InetSocketAddress> unregistered = new AtomicReference<>();
            AtomicReference<Throwable> error = new AtomicReference<>();

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> getSeedNodes(String cluster) {
                    UnsupportedOperationException err = new UnsupportedOperationException();

                    error.set(err);

                    throw err;
                }

                @Override
                public void startDiscovery(String cluster, InetSocketAddress node) {
                    registered.set(node);

                    hangLatch.countDown();

                    await(resumeLatch);
                }

                @Override
                public void stopDiscovery(String cluster, InetSocketAddress node) {
                    unregistered.set(node);

                    resumeLatch.countDown();
                }
            });

            Future<JoinFuture> joinFuture = runAsync(instance::joinAsync);

            await(hangLatch);

            assertNotNull(instance.leaveAsync().get(3, TimeUnit.SECONDS));

            assertNotNull(joinFuture.get().get(3, TimeUnit.SECONDS));

            assertNull(error.get());
            assertNotNull(registered.get());
            assertNotNull(unregistered.get());
            assertEquals(registered.get(), unregistered.get());
        });
    }

    @Test
    public void testTerminateWhileRegisteringSeedNode() throws Exception {
        repeat(50, i -> {
            CountDownLatch hangLatch = new CountDownLatch(1);
            CountDownLatch resumeLatch = new CountDownLatch(1);

            AtomicReference<InetSocketAddress> registered = new AtomicReference<>();
            AtomicReference<InetSocketAddress> unregistered = new AtomicReference<>();
            AtomicReference<Throwable> error = new AtomicReference<>();

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> getSeedNodes(String cluster) {
                    UnsupportedOperationException err = new UnsupportedOperationException();

                    error.set(err);

                    throw err;
                }

                @Override
                public void startDiscovery(String cluster, InetSocketAddress node) {
                    registered.set(node);

                    hangLatch.countDown();

                    await(resumeLatch);
                }

                @Override
                public void stopDiscovery(String cluster, InetSocketAddress node) {
                    unregistered.set(node);

                    resumeLatch.countDown();
                }
            });

            Future<JoinFuture> joinFuture = runAsync(instance::joinAsync);

            await(hangLatch);

            assertNotNull(instance.terminateAsync().get(3, TimeUnit.SECONDS));

            assertNotNull(joinFuture.get().get(3, TimeUnit.SECONDS));

            assertNull(error.get());
            assertNotNull(registered.get());
            assertNotNull(unregistered.get());
            assertEquals(registered.get(), unregistered.get());
        });
    }

    @Test
    public void testUnrecoverableErrorOnJoin() throws Exception {
        repeat(50, i -> {
            AtomicReference<InetSocketAddress> unregistered = new AtomicReference<>();
            AtomicReference<Throwable> error = new AtomicReference<>();

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> getSeedNodes(String cluster) {
                    UnsupportedOperationException err = new UnsupportedOperationException();

                    error.set(err);

                    throw err;
                }

                @Override
                public void startDiscovery(String cluster, InetSocketAddress node) {
                    throw TEST_ERROR;
                }

                @Override
                public void stopDiscovery(String cluster, InetSocketAddress node) {
                    unregistered.set(node);
                }
            });

            try {
                instance.joinAsync().get();

                fail();
            } catch (HekateFutureException e) {
                assertTrue(e.isCausedBy(AssertionError.class));
                assertEquals(TEST_ERROR_MESSAGE, e.getCause().getMessage());
            }

            assertNull(error.get());
            assertNotNull(unregistered.get());
        });
    }

    @Test
    public void testRecoverableErrorOnSeedNodeRegister() throws Exception {
        repeat(5, i -> {
            AtomicInteger errorsCounter = new AtomicInteger(4);

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> getSeedNodes(String cluster) {
                    return Collections.emptyList();
                }

                @Override
                public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
                    if (errorsCounter.getAndDecrement() > 1) {
                        throw new TestHekateException(TEST_ERROR_MESSAGE);
                    }
                }
            });

            instance.join();

            assertEquals(0, errorsCounter.get());

            instance.leave();
        });
    }

    @Test
    public void testRecoverableErrorOnSeedNodeGet() throws Exception {
        repeat(5, i -> {
            AtomicInteger errorsCounter = new AtomicInteger(4);

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> getSeedNodes(String cluster) throws HekateException {
                    if (errorsCounter.getAndDecrement() > 1) {
                        throw new TestHekateException(TEST_ERROR_MESSAGE);
                    }
                    return Collections.emptyList();
                }
            });

            instance.join();

            assertEquals(0, errorsCounter.get());

            instance.leave();
        });
    }
}
