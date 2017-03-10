/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.failover;

import io.hekate.cluster.ClusterNode;
import java.util.Optional;
import java.util.Set;

/**
 * Provides information about an operation failure to {@link FailoverPolicy}.
 */
public interface FailureInfo {
    /**
     * Returns the current value of failover attempts counter (starting with zero for the first attempt).
     *
     * @return Failover attempts count.
     */
    int getAttempt();

    /**
     * Returns {@code true} if this is the first attempt ({@link #getAttempt()} == 0).
     *
     * @return {@code true} if this is the first attempt.
     */
    boolean isFirstAttempt();

    /**
     * Returns the cause of ths failure.
     *
     * @return Cause of this failure.
     */
    Throwable getError();

    /**
     * Returns the last tried node of a failed operation (note that node can be absent due to routing failure).
     *
     * @return Cluster node.
     */
    Optional<ClusterNode> getLastNode();

    /**
     * Returns the immutable set of all failed nodes.
     *
     * @return Immutable set of all failed nodes.
     */
    Set<ClusterNode> getFailedNodes();

    /**
     * Returns the routing policy as it was set by {@link FailoverPolicy}.
     *
     * @return Routing policy.
     *
     * @see FailureResolution#withRoutingPolicy(FailoverRoutingPolicy)
     */
    FailoverRoutingPolicy getRouting();

    /**
     * Returns {@code true} if this failure is caused by an error of the specified type.
     *
     * @param type Error type.
     *
     * @return {@code true} if this failure is caused by an error of the specified type.
     */
    boolean isCausedBy(Class<? extends Throwable> type);
}
