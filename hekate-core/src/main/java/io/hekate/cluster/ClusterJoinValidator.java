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

import io.hekate.core.Hekate;
import java.util.List;

/**
 * Cluster join validator.
 *
 * <p>
 * Implementations of this interface can provide a custom logic of accepting/rejecting new nodes when they are trying to join the
 * cluster.
 * </p>
 *
 * <p>
 * When new node tries to join the cluster it sends a join request to an existing cluster node. When an existing node receives
 * such a request it asks all of its registered validators to check validity of the joining node by calling their {@link
 * #acceptJoin(ClusterNode, Hekate)} method. If any validator returns a non-null reject reason then joining node will be rejected and
 * {@link ClusterJoinRejectedException} will be thrown on its side. Information about the reject reason can be obtained via {@link
 * ClusterJoinRejectedException#rejectReason()} method.
 * </p>
 *
 * <p>
 * Instances of this interface can be registered via {@link ClusterServiceFactory#setJoinValidators(List)} method. Note that the same
 * implementation of this interface must be registered on all nodes within the cluster since any of them can be selected by a joining node
 * as a join target.
 * </p>
 *
 * <p>
 * If multiple instances of this interface are registered within the cluster service then each of them will be called one by one unless one
 * of them decides to reject the joining node.
 * </p>
 *
 * @see ClusterServiceFactory#setJoinValidators(List)
 */
public interface ClusterJoinValidator {
    /**
     * Called when a new node tries to join the cluster. Returns {@code null} if new node should be accepted or an arbitrary string
     * indicating the reject reason.
     *
     * @param joining New node that is trying to join the cluster.
     * @param local {@link Hekate} instance that this acceptor belongs to.
     *
     * @return Reject reason if node should be rejected or {@code null} if node should be accepted.
     */
    String acceptJoin(ClusterNode joining, Hekate local);
}
