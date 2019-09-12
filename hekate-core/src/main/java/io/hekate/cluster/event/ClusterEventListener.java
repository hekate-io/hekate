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

package io.hekate.cluster.event;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Cluster event listener.
 *
 * <h2>Overview</h2>
 * <p>
 * Implementations of this interface can be registered to the {@link ClusterService} in order to get notified on
 * {@link ClusterEvent cluster events}.
 * </p>
 *
 * <p>
 * Listener can be registered at {@link ClusterServiceFactory#setClusterListeners(List) configuration time} or at
 * {@link ClusterService#addListener(ClusterEventListener) runtime}. The key difference between those two approaches is that
 * a configuration-time listener stays registered across multiple restarts of the cluster node, while a runtime-listener stays registered
 * only while the node stays in the cluster and will be unregistered when the node leaves the cluster.
 * </p>
 *
 * <h2>Event types</h2>
 * <p>
 * Below is the list of supported cluster event types:
 * </p>
 * <ul>
 * <li>{@link ClusterJoinEvent} - fired when local node successfully joins the cluster. If listener is registered after the local node
 * had already joined the cluster then this event will be the first event received by the listener and it will contain the most up to date
 * topology information.</li>
 * <li>{@link ClusterChangeEvent} - fired every time when there are changes in the cluster topology. This event includes information about
 * new nodes that joined the cluster and old nodes that left the cluster.</li>
 * <li>{@link ClusterLeaveEvent} - fired when local node leaves the cluster. This event is fired even if the local node gets {@link
 * Hekate#terminate() forcibly terminated}.
 * </li>
 * </ul>
 *
 * <h2>Example</h2>
 * <p>
 * Below is the example of cluster event listener implementation:
 * ${source: cluster/event/ClusterEventListenerJavadocTest.java#cluster_event_listener}
 * </p>
 *
 * @see ClusterService
 * @see ClusterService#addListener(ClusterEventListener)
 * @see ClusterServiceFactory#setClusterListeners(List)
 */
@FunctionalInterface
public interface ClusterEventListener {
    /**
     * Processes the cluster event.
     *
     * <p>
     * Please see the {@link ClusterEventListener overview} section for more details.
     * </p>
     *
     * @param event Cluster event.
     *
     * @throws HekateException If cluster event processing failed.
     * @see ClusterEvent#asJoin()
     * @see ClusterEvent#asChange()
     * @see ClusterEvent#asLeave()
     */
    void onEvent(ClusterEvent event) throws HekateException;
}
