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

package io.hekate.cluster;

import java.util.List;

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
    List<ClusterNode> apply(List<ClusterNode> nodes);
}
