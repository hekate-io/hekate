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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.core.HekateSupport;
import java.util.List;

/**
 * Cluster topology change event.
 *
 * <p>
 * This event gets fired by the {@link ClusterService} every time whenever cluster topology changes are detected. This event includes
 * information about all new nodes that joined the cluster and all old nodes that left the cluster.
 * </p>
 *
 * <p>
 * For more details about cluster events handling please see the documentation of {@link ClusterEventListener} interface.
 * </p>
 *
 * @see ClusterEventListener
 */
public class ClusterChangeEvent extends ClusterEventBase {
    private final List<ClusterNode> added;

    private final List<ClusterNode> removed;

    private final List<ClusterNode> failed;

    /**
     * Constructs a new instance.
     *
     * @param topology Topology.
     * @param added List of newly joined nodes (see {@link #added()}).
     * @param removed List of nodes that left the cluster (see {@link #removed()}).
     * @param failed List of failed nodes (see {@link #failed()}).
     * @param hekate Delegate for {@link #hekate()}.
     */
    public ClusterChangeEvent(
        ClusterTopology topology,
        List<ClusterNode> added,
        List<ClusterNode> removed,
        List<ClusterNode> failed,
        HekateSupport hekate
    ) {
        super(topology, hekate);

        this.added = added;
        this.removed = removed;
        this.failed = failed;
    }

    /**
     * Returns the list of new nodes that joined the cluster.
     *
     * @return List of new nodes that joined the cluster.
     */
    public List<ClusterNode> added() {
        return added;
    }

    /**
     * Returns the list of nodes that left the cluster.
     *
     * <p>
     * This list contains nodes that left the cluster normally <b>and</b> those that were considered to be failed and forcefully removed.
     * </p>
     *
     * <p>
     * For the list of nodes that were forcefully removed from the cluster by the failure detection logic please see the {@link #failed()}
     * method.
     * </p>
     *
     * @return List of nodes that left the cluster.
     */
    public List<ClusterNode> removed() {
        return removed;
    }

    /**
     * Returns the list of failed nodes.
     *
     * <p>
     * This list contains those nodes that were removed from the cluster by the failure detection logic (see {@link FailureDetector}).
     * </p>
     *
     * @return List of failed nodes.
     */
    public List<ClusterNode> failed() {
        return failed;
    }

    /**
     * Returns {@link ClusterEventType#CHANGE}.
     *
     * @return {@link ClusterEventType#CHANGE}.
     */
    @Override
    public ClusterEventType type() {
        return ClusterEventType.CHANGE;
    }

    /**
     * Returns this instance.
     *
     * @return This instance.
     */
    @Override
    public ClusterChangeEvent asChange() {
        return this;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[added=" + added()
            + ", removed=" + removed()
            + ", failed=" + failed()
            + ", topology=" + topology()
            + ']';
    }
}
