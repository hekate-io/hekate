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

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.event.ClusterLeaveReason;
import io.hekate.cluster.seed.SeedNodeProviderAdaptor;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.StaticSeedNodeProviderConfig;
import io.hekate.core.Hekate.State;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.JoinFuture;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.test.HekateTestError;
import io.hekate.test.HekateTestException;
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

public class ClusterServiceSingleNodeTest extends HekateNodeParamTestBase {
    private HekateTestNode node;

    public ClusterServiceSingleNodeTest(HekateTestContext params) {
        super(params);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        node = createNode();
    }

    @Test
    public void testClusterName() throws Exception {
        HekateTestNode node = createNode(boot -> boot.withClusterName("hekate.test-cluster-name")).join();

        assertEquals("hekate.test-cluster-name", node.cluster().clusterName());

        node.leave();

        assertEquals("hekate.test-cluster-name", node.cluster().clusterName());
    }

    @Test
    public void testJoinLeaveEventsWithListenerBeforeJoin() throws Exception {
        repeat(50, i -> {
            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            node.cluster().addListener(listener);

            node.join();

            ClusterNode clusterNode = this.node.localNode();

            node.leave();

            node.cluster().removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).type());
            assertSame(ClusterEventType.LEAVE, events.get(1).type());

            ClusterJoinEvent join = events.get(0).asJoin();

            assertEquals(1, join.topology().size());
            assertEquals(clusterNode, join.topology().localNode());
            assertEquals(clusterNode, join.topology().localNode());
            assertTrue(join.topology().contains(clusterNode));
            assertFalse(join.topology().remoteNodes().contains(clusterNode));

            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, leave.topology().size());
            assertEquals(clusterNode, leave.topology().localNode());
            assertEquals(1, join.topology().localNode().joinOrder());
            assertTrue(leave.topology().contains(clusterNode));
            assertFalse(leave.topology().remoteNodes().contains(clusterNode));
            assertSame(ClusterLeaveReason.LEAVE, leave.reason());
        });
    }

    @Test
    public void testJoinLeaveEventsWithListenerAfterJoin() throws Exception {
        repeat(50, i -> {
            node.join();

            ClusterNode clusterNode = node.localNode();

            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            node.cluster().addListener(listener);

            node.leave();

            node.cluster().removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).type());
            assertSame(ClusterEventType.LEAVE, events.get(1).type());

            ClusterJoinEvent join = events.get(0).asJoin();

            assertEquals(1, join.topology().size());
            assertEquals(clusterNode, join.topology().localNode());
            assertEquals(1, join.topology().localNode().joinOrder());
            assertTrue(join.topology().contains(clusterNode));

            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, leave.topology().size());
            assertEquals(clusterNode, leave.topology().localNode());
            assertEquals(1, leave.topology().localNode().joinOrder());
            assertTrue(leave.topology().contains(clusterNode));
            assertSame(ClusterLeaveReason.LEAVE, leave.reason());
        });
    }

    @Test
    public void testJoinLeaveEventsWithTerminate() throws Exception {
        repeat(50, i -> {
            node.join();

            ClusterNode clusterNode = this.node.localNode();

            List<ClusterEvent> events = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = events::add;

            node.cluster().addListener(listener);

            node.terminate();

            node.cluster().removeListener(listener);

            assertEquals(2, events.size());
            assertSame(ClusterEventType.JOIN, events.get(0).type());
            assertSame(ClusterEventType.LEAVE, events.get(1).type());

            ClusterJoinEvent join = events.get(0).asJoin();

            assertEquals(1, join.topology().size());
            assertEquals(clusterNode, join.topology().localNode());
            assertEquals(1, join.topology().localNode().joinOrder());
            assertTrue(join.topology().contains(clusterNode));

            ClusterLeaveEvent leave = events.get(1).asLeave();

            assertEquals(1, leave.topology().size());
            assertEquals(clusterNode, leave.topology().localNode());
            assertEquals(1, leave.topology().localNode().joinOrder());
            assertTrue(leave.topology().contains(clusterNode));
            assertSame(ClusterLeaveReason.TERMINATE, leave.reason());
        });
    }

    @Test
    public void testStateInListenerBeforeJoin() throws Exception {
        repeat(50, i -> {
            List<State> statuses = new CopyOnWriteArrayList<>();

            ClusterEventListener listener = event -> statuses.add(node.state());

            node.cluster().addListener(listener);

            node.join();

            node.leave();

            node.cluster().removeListener(listener);

            assertEquals(2, statuses.size());
            assertSame(State.SYNCHRONIZING, statuses.get(0));
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
            assertSame(State.UP, get(node.joinAsync()).state());
        } finally {
            seedStopped.set(true);

            // Ping server to wake it up.
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(addr, port), 3000);
            } catch (IOException e) {
                // Ignore.
            }

            get(seedFuture);
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
                public List<InetSocketAddress> findSeedNodes(String cluster) {
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

            Future<JoinFuture> joinFuture = runAsync(node::joinAsync);

            await(hangLatch);

            assertNotNull(get(node.leaveAsync()));

            assertNotNull(get(joinFuture.get()));

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
                public List<InetSocketAddress> findSeedNodes(String cluster) {
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

            Future<JoinFuture> joinFuture = runAsync(node::joinAsync);

            await(hangLatch);

            assertNotNull(get(node.terminateAsync()));

            assertNotNull(get(joinFuture.get()));

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
                public List<InetSocketAddress> findSeedNodes(String cluster) {
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
                node.joinAsync().get();

                fail();
            } catch (HekateFutureException e) {
                assertTrue(e.isCausedBy(AssertionError.class));
                assertEquals(HekateTestError.MESSAGE, e.getCause().getMessage());
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
                public List<InetSocketAddress> findSeedNodes(String cluster) {
                    return Collections.emptyList();
                }

                @Override
                public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
                    if (errorsCounter.getAndDecrement() > 1) {
                        throw new HekateTestException(HekateTestError.MESSAGE);
                    }
                }
            });

            node.join();

            assertEquals(0, errorsCounter.get());

            node.leave();
        });
    }

    @Test
    public void testRecoverableErrorOnSeedNodeGet() throws Exception {
        repeat(5, i -> {
            AtomicInteger errorsCounter = new AtomicInteger(4);

            seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
                @Override
                public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
                    if (errorsCounter.getAndDecrement() > 1) {
                        throw new HekateTestException(HekateTestError.MESSAGE);
                    }
                    return Collections.emptyList();
                }
            });

            node.join();

            assertEquals(0, errorsCounter.get());

            node.leave();
        });
    }
}
