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

package io.hekate.cluster.health;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.HekateException;
import java.util.Collection;
import java.util.Set;

/**
 * <span class="startHere">&laquo; start here</span>Cluster nodes failure detector.
 *
 * <p>
 * Implementations of this interface are responsible for providing failure detection logic to the {@link ClusterService}.
 * Typically this logic is based on heartbeat messages exchange between cluster nodes, however there is no hard restriction and any other
 * algorithms can be used.
 * </p>
 *
 * <p>
 * Below are the key points of how this interface is used by the cluster service:
 * </p>
 *
 * <ul>
 * <li>When cluster service starts or if cluster service detects that there are changes in the cluster topology it calls {@link
 * #update(Set)} method so that failure detector could update its internal state of monitored nodes.</li>
 *
 * <li>Once per {@link #heartbeatInterval() heartbeat interval} cluster service calls {@link #isAlive(ClusterAddress)} method to check
 * if particular remote node is alive. If {@code false} is returned by this method then such node will be marked as suspected to be failed
 * and this information will be shared with other cluster members. If node failure is suspected by the {@link #failureQuorum()}
 * amount of nodes then such node will be marked as failed and will be removed from the cluster.</li>
 *
 * <li>Once per {@link #heartbeatInterval() heartbeat interval} cluster service calls {@link #heartbeatTick()} method.  If this method
 * returns a non-empty list of cluster node addresses then heartbeat request message will be sent to each of those nodes.</li>
 *
 * <li>When cluster service receives a heartbeat request message from a remote node then it calls #{@link
 * #onHeartbeatRequest(ClusterAddress)} method. If {@code true} is returned by this method then heartbeat reply will be sent back to
 * the originator node. Once cluster service of the originator node receives such a reply it calls {@link
 * #onHeartbeatReply(ClusterAddress)} method.</li>
 * </ul>
 *
 * <p>
 * Implementations of this interface can be registered via {@link ClusterServiceFactory#setFailureDetector(FailureDetector)} method.
 * </p>
 *
 * <p>
 * For the default implementation of this interface please see {@link DefaultFailureDetector}.
 * </p>
 *
 * @see DefaultFailureDetector
 * @see ClusterServiceFactory#setFailureDetector(FailureDetector)
 */
public interface FailureDetector {
    /**
     * Initialized this failure detector with the runtime context.
     *
     * @param context Context.
     *
     * @throws HekateException If this failure detector couldn't be initialized.
     */
    void initialize(FailureDetectorContext context) throws HekateException;

    /**
     * Returns the time interval in milliseconds between heartbeat sending rounds (see {@link #heartbeatTick()}).
     *
     * <p>
     * If the returned value if less than or equals to zero then health monitoring will be completely disabled and {@link
     * #heartbeatTick()}/{@link #isAlive(ClusterAddress)} methods will never be called.
     * </p>
     *
     * @return Time interval in milliseconds between heartbeat sending rounds.
     */
    long heartbeatInterval();

    /**
     * Return the amount of nodes that should agree on some particular node failure before removing such node from the cluster.
     *
     * <p>
     * The value of this parameter is expected to be greater than or equals to 1. If values is less then 1 then it will be automatically
     * adjusted to 1.
     * </p>
     *
     * @return Amount of nodes that should agree on some particular node failure before removing such node from the cluster.
     */
    int failureQuorum();

    /**
     * Terminates this failure detector.
     */
    void terminate();

    /**
     * Returns {@code true} if cluster node at the specified address is known to be alive. Returns {@code false} if node is considered to
     * be failed.
     *
     * @param node Node address.
     *
     * @return {@code true} if node is alive or {@code false} if node is considered to be failed.
     */
    boolean isAlive(ClusterAddress node);

    /**
     * Updates this failure detector with the latest information about all known cluster nodes addresses (including local node address).
     *
     * <p>
     * Note that the specified addresses set can include nodes that just started joining and are not within cluster service's {@link
     * ClusterService#topology() topology}.
     * </p>
     *
     * @param nodes Cluster node addresses.
     */
    void update(Set<ClusterAddress> nodes);

    /**
     * Runs a heartbeat tick and returns a set of cluster node addresses that should received a heartbeat request message.
     *
     * <p>
     * The time interval between heartbeat ticks is controlled by {@link #heartbeatInterval()} method.
     * </p>
     *
     * @return Set of cluster node addresses for heartbeat request message sending.
     *
     * @see #onHeartbeatRequest(ClusterAddress)
     */
    Collection<ClusterAddress> heartbeatTick();

    /**
     * Notifies this failure detector on heartbeat request message form a remote node. Returns a boolean flag indicating
     * whether a heartbeat reply should be send ({@code true}) or heartbeat replies are not supported ({@code false}).
     *
     * @param from Address of the heartbeat request sender node.
     *
     * @return {@code true} if heartbeat reply should be send back to the requester.
     *
     * @see #heartbeatTick()
     * @see #onHeartbeatReply(ClusterAddress)
     */
    boolean onHeartbeatRequest(ClusterAddress from);

    /**
     * Notifies this failure detector on heartbeat reply message from a remote node.
     *
     * @param node Address of heartbeat reply sender node.
     */
    void onHeartbeatReply(ClusterAddress node);

    /**
     * Notifies this failure detector upon failure while trying to connect to a remote node.
     *
     * @param node Address of a failed node.
     */
    void onConnectFailure(ClusterAddress node);
}
