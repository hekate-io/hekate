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

import io.hekate.cluster.ClusterView;
import java.util.Optional;

/**
 * Lock region.
 *
 * <p>
 * This interface represents a lock region within the {@link LockService}. For more details about locks and regions please see the
 * documentation of {@link LockService} interface.
 * </p>
 *
 * @see LockService
 */
public interface LockRegion {
    /**
     * Returns the name of this region.
     *
     * @return Name of this region.
     *
     * @see LockService#region(String)
     * @see LockRegionConfig#setName(String)
     */
    String name();

    /**
     * Returns the distributed lock for the specified name.
     *
     * <p>
     * The returned lock object is in unlocked state. Consider using one of its {@link DistributedLock#lock() lock()} method to actually
     * obtain the lock. After lock is not needed anymore it can be released via {@link DistributedLock#unlock() unlock()} method and will
     * be automatically unregistered from the region.
     * </p>
     *
     * @param name Lock name.
     *
     * @return Lock instance.
     */
    DistributedLock get(String name);

    /**
     * Returns information about the node that is currently holding the specified lock.
     *
     * <p>
     * Note that this operation requires a network round trip to the lock manager node and there are no guarantees that lock owner will not
     * change by the time when result is returned from this method.
     * </p>
     *
     * @param name Lock name.
     *
     * @return Lock owner.
     *
     * @throws InterruptedException Signal that current thread was interrupted while awaiting for lock owner information.
     */
    Optional<LockOwnerInfo> ownerOf(String name) throws InterruptedException;

    /**
     * Returns the cluster view of this region.
     *
     * @return Cluster view of this region.
     */
    ClusterView cluster();
}
