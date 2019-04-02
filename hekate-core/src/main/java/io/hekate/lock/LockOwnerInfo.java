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

import io.hekate.cluster.ClusterNode;

/**
 * Provides information about {@link DistributedLock} owner.
 *
 * @see DistributedLock#owner()
 */
public interface LockOwnerInfo {
    /**
     * Returns an identifier of a thread that currently holds the lock.
     *
     * @return Thread identifier.
     *
     * @see Thread#getId()
     */
    long threadId();

    /**
     * Returns a cluster node that currently holds the lock.
     *
     * @return Cluster node that holds the lock.
     */
    ClusterNode node();
}
