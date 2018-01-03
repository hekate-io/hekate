/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.metrics.cluster;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.local.LocalMetricsService;
import java.util.List;
import java.util.Optional;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to cluster-wide metrics API.
 *
 * <h2>Overview</h2>
 * <p>
 * The {@link ClusterMetricsService} provides support for making the {@link LocalMetricsService}'s metrics visible to other nodes of the
 * cluster. It periodically replicates snapshots of metrics data between cluster nodes so that applications can implement their own
 * monitoring and load balancing schemes based on their view of the overall cluster performance.
 * </p>
 *
 * <p>
 * <b>Note:</b> Metrics replication is performed only among those nodes that have metrics replication
 * {@link ClusterMetricsServiceFactory#setEnabled(boolean) enabled}.
 * </p>
 *
 * <p>
 * For more details about metrics and their usage please see the documentation of {@link LocalMetricsService}.
 * </p>
 *
 * <h2>Service configuration</h2>
 * <p>
 * Cluster metrics service can be configured and registered in {@link HekateBootstrap} with the help of {@link
 * ClusterMetricsServiceFactory} as shown in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: metrics/cluster/ClusterMetricsServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: metrics/cluster/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: metrics/cluster/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <p>
 * Note that cluster metrics are disabled by default and should be enabled via the {@link ClusterMetricsServiceFactory#setEnabled(boolean)}
 * configuration option. For other configuration options please see the documentation of {@link ClusterMetricsServiceFactory} class.
 * </p>
 *
 * <h2>Accessing service</h2>
 * <p>
 * Cluster metrics service can be accessed via {@link Hekate#clusterMetrics()} method as in the example below:
 * ${source: metrics/cluster/ClusterMetricsServiceJavadocTest.java#access}
 * </p>
 *
 * <h2>Usage example</h2>
 * <p>
 * The code example below shows how {@link ClusterMetricsService} can be used to access metrics of cluster nodes:
 * ${source: metrics/cluster/ClusterMetricsServiceJavadocTest.java#usage}
 * </p>
 *
 * <h2>Replication protocol details</h2>
 * <p>
 * {@link ClusterMetricsService} uses a periodic synchronization protocol for metrics replication between the cluster nodes.
 * </p>
 *
 * <p>
 * All nodes are organized into a ring with each node periodically sending its own metrics as well as all metrics that it received from its
 * predecessor to the next node. The time interval of metrics replication is controlled by the {@link
 * ClusterMetricsServiceFactory#setReplicationInterval(long)} configuration option.
 * </p>
 *
 * <p>
 * Note that due to the periodic nature of metrics replication algorithm it is possible that some nodes will lag and will see older metrics
 * data than other nodes. How large this lag is depends on the cluster size and the
 * {@link ClusterMetricsServiceFactory#setReplicationInterval(long) replication interval} configuration option.
 * </p>
 *
 * @see ClusterMetricsServiceFactory
 * @see LocalMetricsService
 */
@DefaultServiceFactory(ClusterMetricsServiceFactory.class)
public interface ClusterMetricsService extends Service {
    /**
     * Returns {@code true} if cluster metrics are enabled on this node.
     *
     * <p>
     * Cluster metrics can be enabled/disabled via the {@link ClusterMetricsServiceFactory#setEnabled(boolean)} configuration option.
     * </p>
     *
     * @return {@code true} if cluster metrics are enabled on this node.
     */
    boolean isEnabled();

    /**
     * Returns the metrics of the specified cluster node.
     *
     * @param nodeId Cluster node identifier.
     *
     * @return Metrics.
     */
    Optional<ClusterNodeMetrics> of(ClusterNodeId nodeId);

    /**
     * Returns the metrics of the specified cluster node.
     *
     * @param node Cluster node.
     *
     * @return Metrics.
     */
    Optional<ClusterNodeMetrics> of(ClusterNode node);

    /**
     * Returns the metrics of all nodes known to this service. Returns an empty list if there are no metrics available.
     *
     * @return List of all cluster node metrics or an empty lists.
     */
    List<ClusterNodeMetrics> all();

    /**
     * Returns the metrics of those nodes that have metrics that match the specified filter. Returns an empty list if there are no such
     * nodes.
     *
     * @param filter Filter.
     *
     * @return List of cluster node metrics or an empty lists.
     */
    List<ClusterNodeMetrics> all(MetricFilter filter);
}
