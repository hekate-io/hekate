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

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactoryMock;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.internal.gossip.GossipListener;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateFutureException;
import io.hekate.util.StateGuard;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;

public class HekateTestNode extends HekateNode {
    public static class Bootstrap extends HekateBootstrap {
        public Bootstrap() {
            withService(new ClusterServiceFactoryMock());
        }

        @Override
        public HekateTestNode create() {
            ClusterServiceFactoryMock cluster = service(ClusterServiceFactoryMock.class).get();

            return new HekateTestNode(this, cluster);
        }
    }

    private final List<ClusterEvent> events = new CopyOnWriteArrayList<>();

    private final StateGuard clusterGuard;

    private final ClusterServiceFactoryMock.GossipSpy gossipSpy;

    private ClusterEventListener listener;

    public HekateTestNode(HekateBootstrap cfg, ClusterServiceFactoryMock cluster) {
        super(cfg);

        this.gossipSpy = cluster.getGossipSpy();
        this.clusterGuard = cluster.getServiceGuard();
    }

    public void assertNoNodeFailures() {
        assertFalse(gossipSpy.isHasNodeFailures());
    }

    public StateGuard clusterGuard() {
        return clusterGuard;
    }

    public void setGossipSpy(GossipListener gossipSpy) {
        this.gossipSpy.setDelegate(gossipSpy);
    }

    public synchronized void startRecording() {
        if (listener == null) {
            listener = events::add;

            cluster().addListener(listener);
        }
    }

    public synchronized void stopRecording() {
        if (listener != null) {
            cluster().removeListener(listener);

            listener = null;
        }
    }

    public List<ClusterEvent> events() {
        return new ArrayList<>(events);
    }

    public List<ClusterEvent> events(ClusterEventType type) {
        return events.stream().filter(e -> e.type() == type).collect(toList());
    }

    public ClusterEvent lastEvent() {
        List<ClusterEvent> localEvents = events();

        return localEvents.isEmpty() ? null : localEvents.get(localEvents.size() - 1);
    }

    public void clearEvents() {
        events.clear();
    }

    public void awaitForStatus(State state) throws Exception {
        HekateTestBase.busyWait("node status " + state, () -> state() == state);
    }

    public void awaitForTopology(Hekate... nodes) {
        doAwaitForTopology(Arrays.stream(nodes).map(Hekate::localNode).collect(toList()));
    }

    public void awaitForTopology(Collection<? extends Hekate> nodes) {
        doAwaitForTopology(nodes.stream().map(Hekate::localNode).collect(toList()));
    }

    public void awaitForTopology(ClusterView clusterView, List<ClusterNode> nodes) {
        if (nodes.isEmpty()) {
            return;
        }

        CompletableFuture<ClusterTopology> future = clusterView.futureOf(topology ->
            topology.size() == nodes.size() && topology.nodes().containsAll(nodes) && nodes.containsAll(topology.nodes())
        );

        try {
            future.get(15, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new AssertionError("Thread was interrupted while awaiting for cluster topology:" + nodes, e);
        } catch (ExecutionException e) {
            throw new AssertionError("Failed to await for cluster topology on node " + localNode() + ": " + nodes, e.getCause());
        } catch (TimeoutException e) {
            String msg = "Topology awaiting timed out on node " + localNode() + ": " + nodes
                + "\n\n"
                + HekateTestBase.threadDump();

            throw new AssertionError(msg, e);
        }
    }

    @Override
    public HekateTestNode join() throws HekateFutureException, InterruptedException {
        super.join();

        return this;
    }

    public ClusterTopology topology() {
        return cluster().topology();
    }

    private void doAwaitForTopology(List<ClusterNode> nodes) {
        awaitForTopology(cluster(), nodes);
    }
}
