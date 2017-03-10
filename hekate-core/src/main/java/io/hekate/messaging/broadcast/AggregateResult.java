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

package io.hekate.messaging.broadcast;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.unicast.Reply;
import java.util.Map;
import java.util.Set;

/**
 * Result of {@link MessagingChannel#aggregate(Object) aggregate(...)} operation.
 *
 * @param <T> Base type of aggregation results.
 *
 * @see MessagingChannel#aggregate(Object)
 * @see MessagingChannel#aggregate(Object, AggregateCallback)
 */
public interface AggregateResult<T> {
    /**
     * Returns the original request object that was submitted to cluster nodes.
     *
     * @return Original request.
     */
    T getRequest();

    /**
     * Returns the aggregation participants. Returns an empty set if there were no suitable nodes in the cluster to perform the operation.
     *
     * @return Cluster nodes that participated in the aggregation or an empty set if there were no suitable nodes in the cluster to perform
     * the operation.
     */
    Set<ClusterNode> getNodes();

    /**
     * Returns the map of cluster nodes and errors that happened while trying to communicate with those nodes. Returns an empty map if
     * there were no communication failures.
     *
     * @return Map of nodes and their corresponding failures. Returns an empty map if there were no communication failures.
     *
     * @see #getError(ClusterNode)
     */
    Map<ClusterNode, Throwable> getErrors();

    /**
     * Returns a communication error for the specified node or {@code null} if there was no communication failure with that node.
     *
     * @param node Cluster node (must be one of {@link #getNodes()}, otherwise results will be unpredictable).
     *
     * @return Error in case of a communication error with the specified node or {@code null}.
     *
     * @see #getErrors()
     */
    Throwable getError(ClusterNode node);

    /**
     * Returns {@code true} if aggregation completed successfully without any {@link #getErrors() errors}.
     *
     * @return {@code true} if aggregation completed successfully without any errors.
     *
     * @see #getErrors()
     */
    boolean isSuccess();

    /**
     * Returns {@code true} if there was no communication failure with the specified cluster node.
     *
     * @param node Cluster node (must be one of {@link #getNodes()}, otherwise results will be unpredictable).
     *
     * @return {@code true} if there was no communication failure with the specified cluster node.
     */
    boolean isSuccess(ClusterNode node);

    /**
     * Returns the map of cluster nodes and replies that were successfully received from those nodes. Returns an empty map if there were
     * no successful replies.
     *
     * @return Map of cluster nodes with their replies or an empty map.
     *
     * @see #getReply(ClusterNode)
     */
    Map<ClusterNode, Reply<T>> getReplies();

    /**
     * Returns a reply for the specified cluster node or {@code null} if there was no successful reply from that node.
     *
     * @param node Cluster node (must be one of {@link #getNodes()}, otherwise results will be unpredictable).
     *
     * @return Reply.
     *
     * @see #getReplies()
     */
    Reply<T> getReply(ClusterNode node);
}
