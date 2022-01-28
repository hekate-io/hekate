/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.lock.internal;

import io.hekate.cluster.ClusterView;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockOwnerInfo;
import io.hekate.lock.LockRegion;
import java.util.Optional;

/**
 * Proxy that hides the lifecycle changes of an underlying lock region.
 */
class LockRegionProxy implements LockRegion {
    /** Lock region name. */
    private final String name;

    /** Lock region. */
    private volatile DefaultLockRegion region;

    /**
     * Constructs a new instance.
     *
     * @param name Lock region name.
     */
    public LockRegionProxy(String name) {
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DistributedLock get(String name) {
        return requireRegion().get(name);
    }

    @Override
    public Optional<LockOwnerInfo> ownerOf(String name) throws InterruptedException {
        return requireRegion().ownerOf(name);
    }

    @Override
    public ClusterView cluster() {
        return requireRegion().cluster();
    }

    /**
     * Initializes this proxy with the specified lock region.
     *
     * @param region Lock region.
     */
    public void initialize(DefaultLockRegion region) {
        this.region = region;
    }

    /**
     * Terminates the underlying lock region but keep the reference.
     */
    public void preTerminate() {
        DefaultLockRegion region = this.region;

        if (region != null) {
            region.terminate();
        }
    }

    /**
     * Clears the reference to the underlying lock region.
     */
    public void terminate() {
        this.region = null;
    }

    /**
     * Returns the underlying lock region or throws an error if this proxy is not initialized.
     *
     * @return Lock region.
     *
     * @throws IllegalStateException if proxy is not initialized.
     */
    public DefaultLockRegion requireRegion() {
        // Volatile read.
        DefaultLockRegion region = this.region;

        if (region == null) {
            throw new IllegalStateException("Lock region is not initialized [name=" + name + ']');
        }

        return region;
    }

    @Override
    public String toString() {
        return LockRegion.class.getSimpleName() + "[name=" + name + ']';
    }
}
