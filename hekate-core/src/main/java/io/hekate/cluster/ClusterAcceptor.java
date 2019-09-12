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

import io.hekate.core.Hekate;
import java.util.List;

/**
 * Cluster join acceptor.
 *
 * <p>
 * Implementations of this interface can provide a custom logic of accepting/rejecting new nodes when they are trying to join the
 * cluster.
 * </p>
 *
 * <p>
 * When a new node tries to join the cluster it sends a join request to an existing cluster node. When the node receives such a request it
 * asks all of its registered acceptors to check for the validity of the joining node by calling the {@link
 * #acceptJoin(ClusterNode, Hekate)} method. If any acceptor returns a non-null reject reason then the joining node will be rejected and
 * the {@link ClusterJoinRejectedException} will be thrown on its side. Information about the reject reason can be obtained via the {@link
 * ClusterJoinRejectedException#rejectReason()} method.
 * </p>
 *
 * <p>
 * Instances of this interface can be registered via the {@link ClusterServiceFactory#setAcceptors(List)} method. Note that the same
 * implementation of this interface must be registered on all of the cluster nodes, since any of them can be selected by the joining node
 * as a join target.
 * </p>
 *
 * @see ClusterServiceFactory#setAcceptors(List)
 */
public interface ClusterAcceptor {
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
