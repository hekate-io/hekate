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

package io.hekate.cluster;

import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.StaticSeedNodeProvider;
import io.hekate.cluster.seed.fs.FsSeedNodeProvider;
import io.hekate.cluster.seed.jdbc.JdbcSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.split.AddressReachabilityDetector;
import io.hekate.cluster.split.HostReachabilityDetector;
import io.hekate.cluster.split.JdbcConnectivityDetector;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.cluster.split.SplitBrainDetectorGroup;
import io.hekate.core.Hekate;
import io.hekate.core.Hekate.State;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to clustering API.
 *
 * <h2>Overview</h2>
 * <p>
 * Cluster service provides functionality for getting information about the cluster members and keeping track of membership changes.
 * It uses an eventually consistent <a href="#gossip_protocol">gossip</a>-based protocol for changes propagation among the cluster nodes.
 * Eventual consistency in this context means that node join/leave events are not atomic and can be processed by different
 * members at different time. However it is guaranteed that at some point in time all nodes within the cluster will receive such events
 * and will become aware of cluster topology changes. The time it takes to propagate such events to all cluster members depend on the
 * cluster service configuration options.
 * </p>
 *
 * <ul>
 * <li><a href="#service_configuration">Service Configuration</a></li>
 * <li><a href="#cluster_topology">Cluster Topology</a></li>
 * <li><a href="#cluster_event_listeners">Cluster Event Listener</a></li>
 * <li><a href="#seed_nodes_discovery">Seed Nodes Discovery</a></li>
 * <li><a href="#failure_detection">Failure Detection</a></li>
 * <li><a href="#split_brain_detection">Split-brain Detection</a></li>
 * <li><a href="#cluster_acceptors">Cluster Acceptors</a></li>
 * <li><a href="#gossip_protocol">Gossip Protocol</a></li>
 * </ul>
 *
 * <a name="service_configuration"></a>
 * <h2>Service Configuration</h2>
 * <p>
 * {@link ClusterService} can be configured and registered within the {@link HekateBootstrap} via the {@link ClusterServiceFactory} class
 * as
 * in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: cluster/ClusterServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: cluster/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: cluster/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <p>
 * For more details about the configuration options please see the documentation of {@link ClusterServiceFactory} class.
 * </p>
 *
 * <a name="cluster_topology"></a>
 * <h2>Cluster Topology</h2>
 * <p>
 * Cluster membership information (aka cluster topology) is represented by the {@link ClusterTopology} interface. Instances of this
 * interface can be obtained via {@link #topology()} method. This interface provides various methods for getting information about the
 * cluster nodes based on different criteria (f.e. remote nodes, oldest/youngest node, join order, etc).
 * </p>
 *
 * <p>
 * Each node in the cluster topology is represented by the {@link ClusterNode} interface. This interface provides information about the
 * node's {@link ClusterNode#address() network address}, {@link ClusterNode#services() provided services}, {@link
 * ClusterNode#roles() roles} and {@link ClusterNode#properties() user-defined properties}.
 * </p>
 *
 * <p>
 * Below is the example of accessing the current cluster topology:
 * ${source: cluster/ClusterServiceJavadocTest.java#list_topology}
 * </p>
 *
 * <p>
 * Filtering can be applied to {@link ClusterTopology} by providing an implementation of {@link ClusterNodeFilter} interface as in
 * the example below:
 * ${source: cluster/ClusterServiceJavadocTest.java#filter_topology}
 * </p>
 *
 * <p>
 * Cluster service supports topology versioning by managing a counter that gets incremented every time whenever nodes join or leave the
 * cluster. Value of this counter can be obtained via {@link ClusterTopology#version()} method and can be used to distinguish which
 * topology instance is older and which one is newer.
 * </p>
 *
 * <p>
 * Please note that <b>topology versioning is local to each node</b> and can differ from node to node (i.e. each node maintains its own
 * counter).
 * </p>
 *
 * <a name="cluster_event_listeners"></a>
 * <h2>Cluster Event Listener</h2>
 * <p>
 * Listening for cluster events can be implemented by registering an instance of {@link ClusterEventListener} interface. This can be done
 * at {@link ClusterServiceFactory#withClusterListener(ClusterEventListener) configuration time} or at {@link
 * #addListener(ClusterEventListener) runtime}. The key difference between those two methods is that listeners that were added at
 * configuration time will be kept registered across multiple restarts while listeners that were added at runtime will be kept registered
 * only while node stays in the cluster and will be automatically unregistered when node {@link Hekate#leave() leaves} the cluster.
 * </p>
 *
 * <p>
 * Listeners are notified when local node joins the cluster, leaves the cluster or if cluster service detects membership changes.
 * Multiple concurrent changes are aggregated by the cluster service into a single {@link ClusterEvent}, i.e. if multiple nodes joined or
 * left at the same time then only a single event will be fired holding all the information about all of those nodes and their
 * state.
 * </p>
 *
 * <p>
 * Below is the example of registering a cluster event listener:
 * ${source: cluster/ClusterServiceJavadocTest.java#cluster_event_listener}
 * </p>
 *
 * <p>For more details of cluster events processing please see the documentation of {@link ClusterEventListener} interface.</p>
 *
 * <a name="seed_nodes_discovery"></a>
 * <h2>Seed Nodes Discovery</h2>
 * <p>
 * Whenever local node starts joining the cluster it tries to discover nodes that are already running. If none of such nodes could be
 * found then local node assumes that it is the first node in the cluster and switches to the {@link State#UP UP} state. If some
 * existing nodes could be discovered then local node chooses one of them as a contact node and starts cluster join negotiations
 * with that node.
 * </p>
 *
 * <p>
 * Cluster service uses the {@link SeedNodeProvider} interface for the purpose of existing nodes discovery. Instances of this interface can
 * be registered via {@link ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)} method.
 * </p>
 *
 * <p>
 * The following implementations of this interface are available out of the box:
 * </p>
 * <ul>
 * <li>{@link MulticastSeedNodeProvider} - uses <a href="https://en.wikipedia.org/wiki/IP_multicast" target="_blank">IP multicasting</a>
 * for periodic sending of discovery messages and listening for responses from existing nodes</li>
 * <li>{@link JdbcSeedNodeProvider} - uses a shared RDBMS table as a central repository of running nodes</li>
 * <li>{@link FsSeedNodeProvider} - uses a shared file system folder as a central repository of running nodes</li>
 * <li>{@link StaticSeedNodeProvider} - uses a pre-configured list of network addresses</li>
 * <li>{@link SeedNodeProviderGroup} - groups multiple providers to act as a single provider</li>
 * </ul>
 *
 * <p>Please see the documentation of {@link SeedNodeProvider} for more details on providing custom implementations of this interface.</p>
 *
 * <a name="failure_detection"></a>
 * <h2>Failure Detection</h2>
 * <p>
 * Cluster service relies on {@link FailureDetector} interface for node failure detection. Implementations of this interface are typically
 * using a heartbeat-based approach for failure detection, however other algorithms can also be implemented.
 * </p>
 *
 * <p>
 * Failure detector can be specified via {@link ClusterServiceFactory#setFailureDetector(FailureDetector)} method.
 * </p>
 *
 * <p>
 * Default implementation of this interface is provided by the {@link DefaultFailureDetector} class. This class organizes all nodes into a
 * monitoring ring with fixed heartbeat interval and loss threshold. Please see its javadoc for implementation details and configuration
 * options.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link FailureDetector} interface for more details on implementing  custom failure detection logic.
 * </p>
 *
 * <a name="split_brain_detection"></a>
 * <h2>Split-brain Detection</h2>
 * <p>
 * Cluster service can be configured to automatically detect and perform appropriate actions in case if
 * <a href="https://en.wikipedia.org/wiki/Split-brain_(computing)"target="_blank">split-brain</a> problem arises. Detection is controlled
 * by an implementation of {@link SplitBrainDetector} interface that can be registered within the cluster service via {@link
 * ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)} method.
 * </p>
 *
 * <p>
 * The following implementations of this interface are available out of the box:
 * </p>
 * <ul>
 * <li>{@link AddressReachabilityDetector} - checks connectivity with a pre-configured socket address</li>
 * <li>{@link HostReachabilityDetector} - detector that checks reachability of a pre-configured host address</li>
 * <li>{@link JdbcConnectivityDetector} - detector that checks reachability of a JDBC database</li>
 * </ul>
 *
 * <p>
 * Multiple detectors can be combined with the help of {@link SplitBrainDetectorGroup} class.
 * </p>
 *
 * <p>
 * Actions that should be performed by the cluster service in case of split-brain problem is controlled by the {@link SplitBrainAction}
 * enumeration that can be configured via {@link ClusterServiceFactory#setSplitBrainAction(SplitBrainAction)} method. Please see the
 * documentation of {@link SplitBrainAction} for details about the available options.
 * </p>
 *
 * <a name="cluster_acceptors"></a>
 * <h2>Cluster Acceptors</h2>
 * <p>
 * Whenever a new node tries joins the cluster it can be verified based on some custom application-specific rules (f.e. authorization and
 * permissions checking) and rejected in case of a verification failure.
 * </p>
 *
 * <p>
 * Such verification can be implemented by {@link ClusterServiceFactory#setAcceptors(List) configuring} an implementation of
 * {@link ClusterAcceptor} interface within the cluster service. This interface is used by an existing cluster node when a join
 * request is received from a new node that tries to join the cluster. Implementation of this interface can use the joining node
 * information in order to decide whether the new node should be accepted or it should be rejected. If node gets rejected then it will fail
 * with {@link ClusterJoinRejectedException}.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link ClusterAcceptor} interface for mode details.
 * </p>
 *
 * <a name="gossip_protocol"></a>
 * <h2>Gossip Protocol</h2>
 * <p>
 * Cluster service uses a push-pull <a href="https://en.wikipedia.org/wiki/Gossip_protocol" target="_blank">gossip protocol</a> for
 * membership state management. At high level this protocol can be described as follows:
 * </p>
 *
 * <p>
 * Time to time (based on the configurable interval) each node in the cluster sends a message with its membership information to a
 * number of randomly selected nodes. Every node that receives such a message compares it with its local membership state information.
 * If the received information is more up to date then node updates its local membership view. If local information is more up to date then
 * it is sent back to the originator node (so that it could update its local view with the latest one).
 * </p>
 *
 * <p>
 * Note that when cluster is in convergent state (i.e. membership view is consistent across all cluster nodes) then nodes do not send the
 * whole membership information over the network. In such cases only small membership digests are exchanged.
 * </p>
 *
 * <p>
 * Cluster service supports the concept of speeding up the gossip protocol by using a reactive approach instead of the periodic approach
 * during the membership information exchange. With reactive approach when particular node receives a membership exchange message it
 * immediately forwards such message to a node that haven't seen this membership information yet. Such approach helps to achieve the
 * cluster convergence much faster than with periodic approach, however it requires a much higher network resources utilization.
 * </p>
 *
 * <p>
 * Key configuration options of gossip protocol are:
 * </p>
 * <ul>
 * <li>{@link ClusterServiceFactory#setGossipInterval(long) Gossip interval} - time interval in milliseconds between gossip rounds</li>
 * <li>{@link ClusterServiceFactory#setSpeedUpGossipSize(int) Speed up size} - the maximum amount of nodes in the cluster when gossip
 * protocol can be speeded up by using the reactive approach during messages exchange</li>
 * </ul>
 *
 * @see ClusterServiceFactory
 */
@DefaultServiceFactory(ClusterServiceFactory.class)
public interface ClusterService extends Service, ClusterView {
    /**
     * Returns the cluster name.
     *
     * @return Cluster name.
     *
     * @see HekateBootstrap#setClusterName(String)
     */
    String clusterName();

    /**
     * Returns the local cluster node.
     *
     * @return Local cluster node.
     */
    ClusterNode localNode();
}
