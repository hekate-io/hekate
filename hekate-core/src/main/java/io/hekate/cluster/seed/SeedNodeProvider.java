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

package io.hekate.cluster.seed;

import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Seed node provider.
 *
 * <h2>Overview</h2>
 * <p>
 * Implementations of this interface are responsible for providing seed node addresses to the {@link ClusterService} when it starts joining
 * to the cluster. Seed nodes in this context are nodes that are already running within the cluster and can be used by the newly joining
 * node to send its initial contact information and start the cluster joining negotiations. Due to the decentralized cluster management
 * algorithm any existing node within the cluster can be used as a seed node. Once new node completes its joining to the cluster then it
 * can also be used as a seed node for other nodes.
 * </p>
 *
 * <h2>Generic usage</h2>
 * <p>
 * This interface provides API that can be used to implement various strategies of seed node discovering, f.e. based on group
 * communications protocols (like IP multicast), shared storage (like RDBMS, distributes file system, etc) or simple pre-configured list of
 * addresses. Below are the key points of how this interface is used by the cluster service:
 * </p>
 * <ul>
 * <li>When cluster service starts joining the cluster it calls {@link #startDiscovery(String, InetSocketAddress)} method in order to
 * register the local node address and instruct the seed node provider to start discovering other nodes. Implementations of this method can
 * start sending multicast advertisement messages or register the local node address within some shared central repository.</li>
 *
 * <li>Cluster service periodically calls {@link #findSeedNodes(String)} method and tries to contact each of the returned addresses one by
 * one by sending a join request message.</li>
 *
 * <li>If any of the discovered nodes replies with the join accept message then cluster service calls {@link #suspendDiscovery()} so that
 * seed node provider could stop all activities that are not necessary after alive seed node have been discovered (f.e. stop advertising
 * via multicast). Seed node provider should still keep its local node information registered so that other joining nodes could use
 * the local node as seed node.</li>
 *
 * <li>When cluster service stops it calls {@link #stopDiscovery(String, InetSocketAddress)} method to unregister the local node address
 * and stop the seed node provider.</li>
 * </ul>
 *
 * <h2>Shared state and stale data</h2>
 * <p>
 * If implementation of this interface uses some kind of persistent shared storage for keeping track of alive seed nodes then in case of
 * a particular node crash its address information can still remain registered in the shared storage. In order to clean such stale
 * information the cluster service elects a leader node that periodically scans through all registered addresses, checks their aliveness
 * and removed addresses that are not accessible.
 * </p>
 *
 * <p>
 * Time interval of stale data checking is controlled by the value of {@link #cleanupInterval()} method. If this method returns a
 * positive value then once per such interval the {@link #findSeedNodes(String)} method will be called and each of the returned address will
 * be checked for aliveness (by sending a ping message). If particular address is considered to be failed then it will be unregistered via
 * {@link #unregisterRemote(String, InetSocketAddress)} method. Cluster service will also compare its know topology with the list of
 * addresses and will re-registered missing addresses via {@link #registerRemote(String, InetSocketAddress)}.
 * </p>
 *
 * <h2>Registration</h2>
 * <p>
 * Instances of this interface can be registered within the cluster service via
 * {@link ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)} method.
 * </p>
 *
 * @see ClusterService
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 */
public interface SeedNodeProvider {
    /**
     * Returns the list of known seed node addresses.
     *
     * @param cluster Cluster name (see {@link HekateBootstrap#setClusterName(String)}).
     *
     * @return List of known seed node addresses.
     *
     * @throws HekateException if failed to provide seed node addresses information due to the system failure.
     */
    List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException;

    /**
     * Registers the local node address and starts this provider.
     *
     * @param cluster Cluster name (see {@link HekateBootstrap#setClusterName(String)}).
     * @param node Local node address.
     *
     * @throws HekateException If failed to start discovery due to the system failure.
     */
    void startDiscovery(String cluster, InetSocketAddress node) throws HekateException;

    /**
     * Suspends discovery activities.
     *
     * @throws HekateException If failed to suspend discovery activities due to some system failure.
     */
    void suspendDiscovery() throws HekateException;

    /**
     * Unregisters the local node address and stops this provider.
     *
     * @param cluster Cluster name (see {@link HekateBootstrap#setClusterName(String)}).
     * @param node Local node address.
     *
     * @throws HekateException If failed to stop discovery due to the system failure.
     */
    void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException;

    /**
     * Returns the time interval in milliseconds for the cluster service to perform the stale data cleaning. If the returned value if less
     * than or equals to zero then stale data cleaning will be disabled.
     *
     * @return Time interval in milliseconds.
     */
    long cleanupInterval();

    /**
     * Registered the specified addresses within this provider.
     *
     * <p>
     * This method is the part of a stale data cleanup activity and is performed by the cluster service if it detects that particular node
     * is within its cluster topology but is not registered within this provider (i.e. not returned from {@link #findSeedNodes(String)}
     * method).
     * </p>
     *
     * @param cluster Cluster name (see {@link HekateBootstrap#setClusterName(String)}).
     * @param node Node address that should be registered.
     *
     * @throws HekateException If node couldn't be registered due to the system failure.
     */
    void registerRemote(String cluster, InetSocketAddress node) throws HekateException;

    /**
     * Unregisters the specified address from this provider.
     *
     * <p>
     * This method is the part of a stale data cleanup activity and is called by the cluster service if it detects that there is no cluster
     * node running at the specified address while this address is still registered within this provider (i.e. is returned from {@link
     * #findSeedNodes(String)} method).
     * </p>
     *
     * @param cluster Cluster name (see {@link HekateBootstrap#setClusterName(String)}).
     * @param node Node address that should be unregistered.
     *
     * @throws HekateException If node couldn't be unregistered due to the system failure.
     */
    void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException;
}
