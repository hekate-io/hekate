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

package io.hekate.lock;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.partition.RendezvousHashMapper;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to distributed locks API.
 *
 * <h2>Overview</h2>
 * <p>
 * {@link LockService} provides support for mutually exclusive distributed locks. Each lock can be obtained only by a single node within
 * the cluster. All other nodes that are trying to obtain the same lock will be kept waiting until the lock is released by the owner node
 * or until the owner node goes down and leaves the cluster.
 * </p>
 *
 * <ul>
 * <li><a href="#service_configuration">Service Configuration</a></li>
 * <li><a href="#locks_and_regions">Locks and Regions</a></li>
 * <li><a href="#locking_and_unlocking">Locking and Unlocking</a></li>
 * <li><a href="#protocol_details">Protocol Details</a></li>
 * </ul>
 *
 * <a name="service_configuration"></a>
 * <h2>Service Configuration</h2>
 * <p>
 * {@link LockService} can be registered and configured in {@link HekateBootstrap} with the help of {@link LockServiceFactory} as shown in
 * the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: lock/LockServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: lock/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: lock/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <a name="locks_and_regions"></a>
 * <h2>Locks and Regions</h2>
 * <p>
 * Each lock within the lock service is identified by its {@link DistributedLock#name() name} and a {@link LockRegion}. Lock name is
 * an arbitrary string which can be dynamically constructed and doesn't require any pre-registration within the lock service. Each {@link
 * LockRegion} acts as an independent namespace for locks and must be pre-registered within the {@link LockServiceFactory}. Each region
 * can manage an unlimited amount of locks.
 * </p>
 *
 * <p>
 * Lock regions provide support for distributing workload among the cluster nodes. Only those nodes that have some particular
 * region defined in their configuration will be managing locks for that region. Breaking locks into regions makes it possible to configure
 * cluster nodes so that some subset of cluster nodes will be managing region 'A' while some other subset will be managing region 'B' (note
 * that those subsets can intersect and some nodes from those subsets can manage both regions, i.e. there are no restrictions on how many
 * regions are managed by the particular node).
 * </p>
 *
 * <a name="locking_and_unlocking"></a>
 * <h2>Locking and Unlocking</h2>
 * <p>
 * In order obtain the lock, one must get the {@link LockRegion} instance from the {@link LockService} and then use that instance to create
 * a named {@link DistributedLock} as in the example below:
 * ${source: lock/LockServiceJavadocTest.java#lock}
 * </p>
 *
 * <p>
 * {@link DistributedLock}s are <b>reentrant</b> and it is possible to call {@link DistributedLock#lock() lock()} while the same
 * lock (with the same region and name) is already held by the current thread. In such case {@link DistributedLock#lock() lock()} method
 * will return immediately without accessing remote nodes. However, please note that {@link DistributedLock#lock() lock()} method calls
 * must always be followed by the same amount of {@link DistributedLock#unlock() unlock()} method calls or the lock will never be released.
 * </p>
 *
 * <a name="protocol_details"></a>
 * <h2>Protocol Details</h2>
 * <p>
 * {@link LockService} uses {@link RendezvousHashMapper} to evenly distribute locks processing workload among the nodes. For each
 * lock/unlock operation it selects a node that is responsible for managing the lock with the given name and forwards those operations to
 * such node. Manager node controls the lifecycle and order of locks and makes sure that locks are released if lock owner node prematurely
 * leaves the cluster before properly releasing the lock.
 * </p>
 *
 * <p>
 * During the normal work each lock/unlock operation is performed within a single network round trip from the lock requester to the lock
 * manager. In case of the cluster topology changes all lock/unlock operations are suspended until the cluster rebalancing is finished and
 * live locks are migrated among the cluster members. The time it takes to perform rebalancing depends on the number of nodes within the
 * lock region and the amount of acquired locks that require migration. Note that {@link LockService} doesn't keep track of released locks
 * and removes them from memory, thus only those locks that are in the LOCKED state at the time of rebalancing are migrated.
 * </p>
 *
 * <p>
 * Locks rebalancing is controlled by the cluster coordinator which is dynamically selected among the cluster nodes. Coordinator uses a
 * two-phase lock migration protocol. During the the first phase it collects information about all locks that require migration by sending
 * a 'prepare' message over the nodes ring. This message circulates over the ring so that each node could inspect its local state and
 * decide which of its owned locks require migration (i.e. which lock was re-mapped to another manager node). When message returns back to
 * the coordinator it contains information about all locks in the region that require migration. During the second phase, coordinator sends
 * an 'apply' message over the ring. This message contains information about migrating locks and is used by the cluster nodes to change
 * their lock management state (i.e. take control over newly assigned locks and unload locks that were re-mapped to some other manager
 * node).
 * </p>
 *
 * @see LockServiceFactory
 */
@DefaultServiceFactory(LockServiceFactory.class)
public interface LockService extends Service {
    /**
     * Returns all lock regions that are {@link LockServiceFactory#setRegions(List)} registered within this service.
     *
     * @return Lock regions or an empty collection if there are no registered regions.
     */
    List<LockRegion> allRegions();

    /**
     * Returns a lock region for the specified name.
     *
     * <p>
     * Lock region with the specified name must be registered within the {@link LockServiceFactory} otherwise an error will be thrown.
     * </p>
     *
     * @param region Region name (see {@link LockRegionConfig#setName(String)}).
     *
     * @return Lock region.
     *
     * @see LockServiceFactory#withRegion(LockRegionConfig)
     */
    LockRegion region(String region);

    /**
     * Returns {@code true} if this service has a lock region with the specified name.
     *
     * @param region Region name (see {@link LockRegionConfig#setName(String)}).
     *
     * @return {@code true} if region exists.
     */
    boolean hasRegion(String region);
}
