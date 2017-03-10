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

package io.hekate.profiling;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.DefaultFailureDetectorConfig;
import io.hekate.cluster.seed.SeedNodeProviderAdaptor;
import io.hekate.cluster.seed.SeedNodeProviderMock;
import io.hekate.core.Hekate;
import io.hekate.core.HekateTestInstance;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.internal.HekateInstance;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.junit.Before;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ClusterPerformanceTest extends HekateTestBase {
    public static final int GOSSIP_INTERVAL = 1000;

    public static final int SPEED_UP_GOSSIP_SIZE = 1000;

    public static final int HEARTBEAT_INTERVAL = 1000;

    public static final int HEARTBEAT_LOSS_THRESHOLD = 3;

    private SeedNodeProviderMock seedNodes;

    @Before
    public void setUp() {
        seedNodes = new SeedNodeProviderMock();
    }

    @Test
    public void testSequential() throws Exception {
        List<HekateTestInstance> instances = new ArrayList<>();

        sayHeader("Starting");

        repeat(100, i -> sayTime("Start " + i, () -> {
            InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), 20000 + i);

            HekateTestInstance inst = createInstance(addr);

            inst.join();

            instances.add(inst);

            for (HekateTestInstance other : instances) {
                other.awaitForTopology(instances);
            }
        }));

        sayHeader("Stopping");

        int idx = 0;

        for (Iterator<HekateTestInstance> it = instances.iterator(); it.hasNext(); ) {
            HekateTestInstance inst = it.next();

            it.remove();

            sayTime("Leave " + idx++ + " ~ " + inst.getNode().getId(), () -> {
                inst.leave();

                for (HekateTestInstance clusterService : instances) {
                    clusterService.awaitForTopology(instances);
                }
            });
        }
    }

    @Test
    public void testSequentialBatch() throws Exception {
        List<HekateTestInstance> instances = new ArrayList<>();

        sayHeader("Starting");

        AtomicInteger port = new AtomicInteger(20000);

        int batches = 10;
        int nodesPerBatch = 10;

        repeat(batches, i -> sayTime("Batch " + i, () -> {
            List<JoinFuture> joinFutures = new ArrayList<>();

            repeat(nodesPerBatch, j -> {
                InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), port.incrementAndGet());

                HekateTestInstance inst = createInstance(addr);

                instances.add(inst);

                joinFutures.add(inst.joinAsync());
            });

            for (JoinFuture future : joinFutures) {
                future.get();
            }

            for (HekateTestInstance clusterService : instances) {
                clusterService.awaitForTopology(instances);
            }
        }));

        sayHeader("Stopping");

        List<LeaveFuture> allLeaves = new ArrayList<>();

        repeat(batches, i -> sayTime("Batch " + i, () -> {
            repeat(nodesPerBatch, j -> {
                HekateTestInstance inst = instances.remove(instances.size() - 1);

                allLeaves.add(inst.leaveAsync());
            });

            for (HekateTestInstance clusterService : instances) {
                clusterService.awaitForTopology(instances);
            }
        }));

        for (LeaveFuture leave : allLeaves) {
            leave.get();
        }
    }

    @Test
    public void testRandom() throws Throwable {
        HekateTestInstance seed = createInstance(new InetSocketAddress(InetAddress.getLocalHost(), 12001));

        seed.join();

        seedNodes.setDelegate(new SeedNodeProviderAdaptor() {
            @Override
            public List<InetSocketAddress> getSeedNodes(String cluster) {
                return Collections.singletonList(seed.getNode().getNetAddress());
            }
        });

        int nodes = 100;

        AtomicBoolean stopped = new AtomicBoolean();
        CountDownLatch errorLatch = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        ExecutorService pool = Executors.newFixedThreadPool(nodes);

        List<HekateTestInstance> instances = new ArrayList<>();

        List<Future<?>> tasks = new ArrayList<>();

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        for (int i = 0; i < nodes; i++) {
            int port = 11000 + i;

            HekateTestInstance inst = createInstance(new InetSocketAddress(InetAddress.getLocalHost(), port));

            instances.add(inst);

            Future<Void> task = pool.submit(() -> {
                try {
                    while (!stopped.get()) {
                        Thread.sleep(new Random().nextInt(10000));

                        lock.readLock().lock();

                        try {
                            sayTime("Started: " + port, () -> {
                                Hekate started = inst.joinAsync().get(30, TimeUnit.SECONDS);

                                if (started == null) {
                                    ClusterNodeId node = null;

                                    try {
                                        node = inst.getNode().getId();
                                    } catch (IllegalStateException e) {
                                        // No-op.
                                    }

                                    inst.terminate();

                                    throw new IllegalStateException("Failed to await for node join: " + port + " (" + node + ')');
                                }
                            });
                        } finally {
                            lock.readLock().unlock();
                        }

                        int sleep = new Random().nextInt(10000);

                        Thread.sleep(sleep);

                        lock.readLock().lock();

                        try {
                            ClusterNodeId node = null;

                            try {
                                node = inst.getNode().getId();
                            } catch (IllegalStateException e) {
                                // No-op.
                            }

                            final ClusterNodeId finalNode = node;

                            sayTime("Stopped: " + port, () -> {
                                Hekate stopped1 = inst.leaveAsync().get(30, TimeUnit.SECONDS);

                                if (stopped1 == null) {
                                    inst.terminate();

                                    throw new IllegalStateException("Failed to await for node leave: " + port + " (" + finalNode + ')');
                                }
                            });
                        } finally {
                            lock.readLock().unlock();
                        }
                    }
                } catch (Throwable e) {
                    error.compareAndSet(null, e);

                    errorLatch.countDown();
                }

                return null;
            });

            tasks.add(task);
        }

        for (int i = 0; i < 20; i++) {
            if (errorLatch.await(5, TimeUnit.SECONDS)) {
                break;
            }

            lock.writeLock().lock();

            try {
                List<HekateTestInstance> nonDown = instances.stream()
                    .filter(inst -> inst.getState() != Hekate.State.DOWN)
                    .collect(toList());

                nonDown.add(seed);

                sayTime("Consistent topology of " + nonDown.size() + " nodes", () -> nonDown.forEach(inst -> {
                    assertSame(Hekate.State.UP, inst.getState());

                    inst.awaitForTopology(nonDown);
                }));
            } finally {
                lock.writeLock().unlock();
            }
        }

        stopped.set(true);

        for (Future<?> task : tasks) {
            task.get();
        }

        for (HekateInstance instance : instances) {
            instance.leave();
        }

        seed.leave();

        pool.shutdown();

        assertTrue(pool.awaitTermination(3, TimeUnit.SECONDS));

        if (error.get() != null) {
            throw error.get();
        }
    }

    @Test
    public void testSequentialWithTerminate() throws Exception {
        List<HekateTestInstance> instances = new ArrayList<>();

        sayHeader("Starting");

        repeat(100, i -> sayTime("Start " + i, () -> {
            InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), 20000 + i);

            HekateTestInstance inst = createInstance(addr);

            inst.join();

            instances.add(inst);

            for (HekateTestInstance clusterService : instances) {
                clusterService.awaitForTopology(instances);
            }
        }));

        sayHeader("Terminating");

        int idx = 0;

        for (Iterator<HekateTestInstance> it = instances.iterator(); it.hasNext(); ) {
            HekateTestInstance inst = it.next();

            it.remove();

            sayTime("Terminate " + idx++, () -> {
                inst.terminate();

                for (HekateTestInstance clusterService : instances) {
                    clusterService.awaitForTopology(instances);
                }
            });
        }
    }

    @Test
    public void testJoinLeaveTime() throws Exception {
        List<HekateTestInstance> clusters = new ArrayList<>();
        List<JoinFuture> joins = new ArrayList<>();

        // Non-existing seed nodes.
        // seedNodes.startDiscovery(new ClusterAddress(new InetSocketAddress(InetAddress.getLocalHost(), 30001), newNodeId()));
        // seedNodes.startDiscovery(new ClusterAddress(new InetSocketAddress(InetAddress.getLocalHost(), 30002), newNodeId()));
        // seedNodes.startDiscovery(new ClusterAddress(new InetSocketAddress(InetAddress.getLocalHost(), 30003), newNodeId()));

        long t1 = System.currentTimeMillis();

        ClusterEventListener listener = new ClusterEventListener() {
            private final AtomicInteger cnt = new AtomicInteger();

            @Override
            public void onEvent(ClusterEvent event) {
                switch (event.getType()) {
                    case JOIN: {
                        say("Joined " + cnt.incrementAndGet());

                        break;
                    }
                    case LEAVE: {
                        say("Left " + cnt.decrementAndGet());

                        break;
                    }
                    case CHANGE: {
                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unsupported event type: " + event);
                    }
                }
            }
        };

        for (int i = 1; i <= 100; i++) {
            InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), 20000 + i);

            HekateTestInstance h = createInstance(addr);

            h.get(ClusterService.class).addListener(listener);

            joins.add(h.joinAsync());

            clusters.add(h);
        }

        for (JoinFuture wait : joins) {
            wait.get();
        }

        sayTime("Awaiting consistent topology", () -> {
            for (HekateTestInstance clusterService : clusters) {
                clusterService.awaitForTopology(clusters);
            }
        });

        long t2 = System.currentTimeMillis();

        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLocalHost(), 10000);

        Hekate h = createInstance(addr);

        h.get(ClusterService.class).addListener(listener);

        h.join();

        say("LEAVE");

        long t3 = System.currentTimeMillis();

        h.leave();

        long t4 = System.currentTimeMillis();

        say("LEAVE DONE");

        List<LeaveFuture> leaves = clusters.stream().map(Hekate::leaveAsync).collect(toList());

        for (LeaveFuture wait : leaves) {
            wait.get();
        }

        long t5 = System.currentTimeMillis();

        say("Join time: " + (t2 - t1));
        say("Last join time: " + (t3 - t2));
        say("Last leave time: " + (t4 - t3));
        say("Leave time: " + (t5 - t4));
    }

    private HekateTestInstance createInstance(InetSocketAddress addr) {
        HekateTestInstance.Bootstrap bootstrap = new HekateTestInstance.Bootstrap(addr);

        bootstrap.setClusterName("test");
        bootstrap.setNodeName("node" + addr.getPort());

        bootstrap.withNodeRole("Role1");
        bootstrap.withNodeRole("Role2");
        bootstrap.withNodeRole("Role3");

        bootstrap.withNodeProperty("P1", "V1");
        bootstrap.withNodeProperty("P2", "V2");
        bootstrap.withNodeProperty("P3", "V3");

        NetworkServiceFactory net = new NetworkServiceFactory();

        net.setHost(addr.getAddress().getHostAddress());
        net.setPort(addr.getPort());
        net.setHeartbeatInterval(HEARTBEAT_INTERVAL);
        net.setHeartbeatLossThreshold(HEARTBEAT_LOSS_THRESHOLD);
        net.setNioThreads(2);
        net.setTcpNoDelay(true);
        net.setTcpReuseAddress(false);

        bootstrap.withService(net);

        ClusterServiceFactory cluster = bootstrap.find(ClusterServiceFactory.class).get();

        cluster.setGossipInterval(GOSSIP_INTERVAL);
        cluster.setSpeedUpGossipSize(SPEED_UP_GOSSIP_SIZE);

        cluster.setSeedNodeProvider(seedNodes);

        DefaultFailureDetectorConfig fdCfg = new DefaultFailureDetectorConfig();

        fdCfg.setHeartbeatInterval(HEARTBEAT_INTERVAL);
        fdCfg.setHeartbeatLossThreshold(HEARTBEAT_LOSS_THRESHOLD);

        cluster.setFailureDetector(new DefaultFailureDetector(fdCfg));

        return bootstrap.createInstance();
    }
}
