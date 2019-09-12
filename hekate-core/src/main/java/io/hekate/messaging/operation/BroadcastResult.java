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

package io.hekate.messaging.operation;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.MessagingChannel;
import java.util.List;
import java.util.Map;

/**
 * Result of a {@link Broadcast} operation.
 *
 * @param <T> Base type of broadcast message.
 *
 * @see MessagingChannel#newBroadcast(Object)
 */
public interface BroadcastResult<T> {
    /**
     * Returns the original message object that was submitted to the cluster.
     *
     * @return Original message.
     */
    T message();

    /**
     * Returns the broadcast operation participants. Returns an empty list if there were no suitable nodes in the cluster to perform the
     * operation.
     *
     * @return Cluster nodes that participated in the broadcast operation or an empty list if there were no suitable nodes in the cluster to
     * perform the operation.
     */
    List<ClusterNode> nodes();

    /**
     * Returns the map of cluster nodes and errors that happened while trying to communicate with these nodes. Returns an empty map if
     * there were no communication failures.
     *
     * @return Map of nodes and their corresponding failures. Returns an empty map if there were no communication failures.
     *
     * @see #errorOf(ClusterNode)
     */
    Map<ClusterNode, Throwable> errors();

    /**
     * Returns {@code true} if broadcast completed successfully without any {@link #errors() errors}.
     *
     * @return {@code true} if broadcast completed successfully without any errors.
     *
     * @see #errors()
     */
    default boolean isSuccess() {
        Map<ClusterNode, Throwable> errors = errors();

        return errors == null || errors.isEmpty();
    }

    /**
     * Returns {@code true} if there was no communication failure with the specified cluster node..
     *
     * @param node Cluster node (must be one of {@link #nodes()}, otherwise results will be unpredictable).
     *
     * @return {@code true} if there was no communication failure with the specified cluster node.
     */
    default boolean isSuccess(ClusterNode node) {
        return errorOf(node) == null;
    }

    /**
     * Returns a communication error for the specified node or {@code null} if there was no communication failure with that node.
     *
     * @param node Cluster node (must be one of {@link #nodes()}, otherwise results will be unpredictable).
     *
     * @return Error in case of a communication error with the specified node or {@code null}.
     *
     * @see #errors()
     */
    default Throwable errorOf(ClusterNode node) {
        Map<ClusterNode, Throwable> errors = errors();

        return errors != null ? errors.get(node) : null;
    }
}
