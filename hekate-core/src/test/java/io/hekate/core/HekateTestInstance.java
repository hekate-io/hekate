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

package io.hekate.core;

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactoryMock;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.internal.gossip.GossipListener;
import io.hekate.core.internal.HekateInstance;
import io.hekate.util.StateGuard;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class HekateTestInstance extends HekateInstance {
    public static class Bootstrap extends HekateBootstrap {
        private final InetSocketAddress address;

        public Bootstrap(InetSocketAddress address) {
            this.address = address;

            withService(new ClusterServiceFactoryMock());
        }

        @Override
        public HekateTestInstance createInstance() {
            ClusterServiceFactoryMock cluster = find(ClusterServiceFactoryMock.class).get();

            return new HekateTestInstance(address, this, cluster);
        }
    }

    private final List<ClusterEvent> events = new CopyOnWriteArrayList<>();

    private final InetSocketAddress socketAddress;

    private final StateGuard clusterGuard;

    private final ClusterServiceFactoryMock.GossipSpy gossipSpy;

    private ClusterEventListener listener;

    public HekateTestInstance(InetSocketAddress socketAddress, HekateBootstrap cfg, ClusterServiceFactoryMock cluster) {
        super(cfg);

        this.socketAddress = socketAddress;
        this.gossipSpy = cluster.getGossipSpy();
        this.clusterGuard = cluster.getServiceGuard();
    }

    public void assertNoNodeFailures() {
        assertFalse(gossipSpy.isHasNodeFailures());
    }

    public StateGuard getClusterGuard() {
        return clusterGuard;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public void setGossipSpy(GossipListener gossipSpy) {
        this.gossipSpy.setDelegate(gossipSpy);
    }

    public synchronized void startRecording() {
        if (listener == null) {
            listener = events::add;

            get(ClusterService.class).addListener(listener);
        }
    }

    public synchronized void stopRecording() {
        if (listener != null) {
            get(ClusterService.class).removeListener(listener);

            listener = null;
        }
    }

    public List<ClusterEvent> getEvents() {
        return new ArrayList<>(events);
    }

    public List<ClusterEvent> getEvents(ClusterEventType type) {
        return events.stream().filter(e -> e.getType() == type).collect(toList());
    }

    public ClusterEvent getLastEvent() {
        List<ClusterEvent> localEvents = getEvents();

        return localEvents.isEmpty() ? null : localEvents.get(localEvents.size() - 1);
    }

    public void clearEvents() {
        events.clear();
    }

    public void awaitForStatus(State state) throws Exception {
        HekateTestBase.busyWait("node status " + state, () -> getState() == state);
    }

    public void awaitForTopology(Hekate... nodes) {
        doAwaitForTopology(Arrays.stream(nodes).map(Hekate::getNode).collect(toList()));
    }

    public void awaitForTopology(ClusterNode... nodes) {
        doAwaitForTopology(Arrays.asList(nodes));
    }

    public void awaitForTopology(List<? extends Hekate> nodes) {
        doAwaitForTopology(nodes.stream().map(Hekate::getNode).collect(toList()));
    }

    public void awaitForTopology(ClusterView clusterView, List<ClusterNode> nodes) {
        if (nodes.isEmpty()) {
            return;
        }

        CompletableFuture<ClusterTopology> future = clusterView.futureOf(topology ->
            topology.size() == nodes.size() && topology.getNodes().containsAll(nodes) && nodes.containsAll(topology.getNodes())
        );

        try {
            future.get(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            fail("Thread was interrupted while awaiting for cluster topology:" + nodes);
        } catch (ExecutionException e) {
            throw new AssertionError("Failed to await for cluster topology: " + nodes, e.getCause());
        } catch (TimeoutException e) {
            fail("Failed to await for cluster topology: " + nodes);
        }
    }

    @Override
    public HekateTestInstance join() throws HekateFutureException, InterruptedException {
        super.join();

        return this;
    }

    public ClusterTopology getTopology() {
        return get(ClusterService.class).getTopology();
    }

    private void doAwaitForTopology(List<ClusterNode> nodes) {
        awaitForTopology(get(ClusterService.class), nodes);
    }
}
