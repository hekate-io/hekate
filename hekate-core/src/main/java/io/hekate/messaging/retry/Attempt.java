/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.messaging.retry;

import io.hekate.cluster.ClusterNode;
import java.util.Set;

/**
 * Attempt.
 */
public interface Attempt {
    /**
     * Returns the current attempts (starting with zero for the first attempt).
     *
     * @return Attempts count.
     */
    int attempt();

    /**
     * Returns the last tried node.
     *
     * @return Last tried node.
     */
    ClusterNode lastTriedNode();

    /**
     * Returns an immutable set of all tried nodes.
     *
     * @return Immutable set of all tried nodes.
     */
    Set<ClusterNode> allTriedNodes();

    /**
     * Returns {@code true} if the specified node is in the {@link #allTriedNodes()} set.
     *
     * @param node Node to check.
     *
     * @return {@code true} if the specified node is in the {@link #allTriedNodes()} set.
     */
    default boolean hasTriedNode(ClusterNode node) {
        return allTriedNodes().contains(node);
    }

    /**
     * Returns {@code true} if this is the first attempt ({@link #attempt()} == 0).
     *
     * @return {@code true} if this is the first attempt.
     */
    default boolean isFirstAttempt() {
        return attempt() == 0;
    }
}
