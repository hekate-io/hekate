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

package io.hekate.cluster.split;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.ClusterSplitBrainException;
import io.hekate.core.HekateFatalErrorPolicy;

/**
 * Cluster split-brain detector.
 *
 * <p>
 * This interface represents a component which is responsible for <a href="https://en.wikipedia.org/wiki/Split-brain_(computing)"
 * target="_blank">cluster split-brain</a> detection.
 * </p>
 *
 * <p>
 * Implementations of this component should perform a quick check (possibly by consulting with some shared resource) and verify that local
 * node can reach other nodes.
 * </p>
 *
 * <p>
 * Cluster service calls this component when any of the following events happens:
 * </p>
 * <ul>
 * <li>Right before the local node joins to the cluster</li>
 * <li>When some other node leaves the cluster</li>
 * <li>Periodically, if {@link ClusterServiceFactory#setSplitBrainCheckInterval(long)} is set to a positive value</li>
 * </ul>
 *
 * <p>
 * If this component detects that local node can't reach other nodes then {@link HekateFatalErrorPolicy} will be applied
 * with {@link ClusterSplitBrainException} as a cause.
 * </p>
 *
 * <p>
 * Note that it is possible combine multiple detectors with the help of {@link SplitBrainDetectorGroup}.
 * </p>
 *
 * @see ClusterServiceFactory#setSplitBrainDetector(SplitBrainDetector)
 */
public interface SplitBrainDetector {
    /**
     * Performs a split-brain check and returns {@code true} if local node can reach other members of the cluster.
     *
     * <p>
     * If this method returns {@code false} then {@link HekateFatalErrorPolicy} will be applied with {@link ClusterSplitBrainException}
     * as a cause.
     * </p>
     *
     * @param localNode Local node (where the check is performed).
     *
     * @return {@code true} if local node can reach other members of the cluster.
     */
    boolean isValid(ClusterNode localNode);
}
