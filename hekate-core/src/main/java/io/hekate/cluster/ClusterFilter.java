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

package io.hekate.cluster;

import io.hekate.core.internal.util.ArgAssert;
import java.util.Collection;
import java.util.Set;

/**
 * Interface for filtering groups of cluster nodes.
 *
 * @see ClusterFilterSupport#filterAll(ClusterFilter)
 */
@FunctionalInterface
public interface ClusterFilter {
    /**
     * Applies this filter to the specified group of cluster nodes and returns a new set of only those nodes that were accepted by this
     * filter.
     *
     * @param nodes Nodes to filter.
     *
     * @return New set of nodes that were accepted by this filter or an empty set if none of the nodes were accepted.
     */
    Set<ClusterNode> apply(Collection<ClusterNode> nodes);

    /**
     * Combines two filters with AND logic and produces a new filter that will accept only those nodes that match both of the specified
     * filters.
     *
     * @param f1 First filter.
     * @param f2 Second filter.
     *
     * @return New filter.
     */
    static ClusterFilter and(ClusterFilter f1, ClusterFilter f2) {
        ArgAssert.notNull(f1, "First filter");
        ArgAssert.notNull(f2, "Second filter");

        return nodes -> {
            Set<ClusterNode> filtered = f1.apply(nodes);

            if (filtered.isEmpty()) {
                return filtered;
            }

            return f2.apply(filtered);
        };
    }
}
