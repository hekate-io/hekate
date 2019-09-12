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

package io.hekate.core.service;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.core.Hekate;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Context for {@link ClusterService}.
 *
 * <p>
 * This interface is attended to providing feedback from a {@link ClusterService} back to the {@link Hekate} instance. However
 * other services can also use it in order to obtain current cluster topology or register cluster event listeners.
 * </p>
 *
 * @see InitializationContext#cluster()
 */
public interface ClusterContext {
    /**
     * Asynchronously notifies this context that {@link ClusterService} has started joining the cluster.
     *
     * @return Future object that gets completed once notification is processed.
     */
    CompletableFuture<Boolean> onStartJoining();

    /**
     * Asynchronously notifies this context that {@link ClusterService} has successfully joined the cluster.
     *
     * @param joinOrder Join order of the local node (see {@link ClusterNode#joinOrder()}).
     * @param newTopology Initial topology.
     *
     * @return Future object that gets completed once notification is processed.
     */
    CompletableFuture<ClusterJoinEvent> onJoin(int joinOrder, Set<ClusterNode> newTopology);

    /**
     * Fires the {@link ClusterChangeEvent}.
     *
     * @param live Nodes that are alive.
     * @param failed Nodes that are known to be failed (i.e. left the cluster abnormally without going through the cluster leave protocol).
     *
     * @return Future object that gets completed after processing the cluster event.
     */
    CompletableFuture<ClusterChangeEvent> onTopologyChange(Set<ClusterNode> live, Set<ClusterNode> failed);

    /**
     * Asynchronously notifies on {@link ClusterService} successfully left the cluster.
     */
    void onLeave();

    /**
     * Returns the current cluster topology.
     *
     * @return Current cluster topology.
     */
    ClusterTopology topology();

    /**
     * Synchronously registers the specified cluster event listener.
     *
     * @param listener Cluster listener.
     */
    void addListener(ClusterEventListener listener);

    /**
     * Synchronously registers the specified cluster event listener.
     *
     * @param listener Cluster listener.
     * @param eventTypes Event types to listen for.
     */
    void addListener(ClusterEventListener listener, ClusterEventType... eventTypes);

    /**
     * Asynchronously registers the specified cluster event listener without awaiting for registration to be completed.
     *
     * @param listener Cluster listener.
     */
    void addListenerAsync(ClusterEventListener listener);

    /**
     * Asynchronously registers the specified cluster event listener without awaiting for registration to be completed.
     *
     * @param listener Cluster listener.
     * @param eventTypes Event types to listen for.
     */
    void addListenerAsync(ClusterEventListener listener, ClusterEventType... eventTypes);

    /**
     * Synchronously removes the specified cluster event listener.
     *
     * @param listener Listener to be removed.
     */
    void removeListener(ClusterEventListener listener);

    /**
     * Registers the synchronization future that will block the {@link Hekate#join()} method until synchronization is complete.
     *
     * @param future Future.
     */
    void addSyncFuture(CompletableFuture<?> future);
}
