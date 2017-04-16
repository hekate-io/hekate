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
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

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
     * Returns the original request object that was submitted to the cluster.
     *
     * @return Original request.
     */
    T request();

    /**
     * Returns the aggregation participants. Returns an empty set if there were no suitable nodes in the cluster to perform the operation.
     *
     * @return Cluster nodes that participated in the aggregation or an empty set if there were no suitable nodes in the cluster to perform
     * the operation.
     */
    Set<ClusterNode> nodes();

    /**
     * Returns the map of cluster nodes and errors that happened while trying to communicate with those nodes. Returns an empty map if
     * there were no failures.
     *
     * @return Map of nodes and their corresponding failures. Returns an empty map if there were no failures.
     *
     * @see #errorOf(ClusterNode)
     */
    Map<ClusterNode, Throwable> errors();

    /**
     * Returns an error for the specified node or {@code null} if there was no failure on that node.
     *
     * @param node Cluster node (must be one of the {@link #nodes()}).
     *
     * @return Error in case of a communication error with the specified node or {@code null}.
     *
     * @see #errors()
     */
    Throwable errorOf(ClusterNode node);

    /**
     * Returns {@code true} if aggregation completed successfully without any {@link #errors() errors}.
     *
     * @return {@code true} if aggregation completed successfully without any errors.
     *
     * @see #errors()
     */
    boolean isSuccess();

    /**
     * Returns {@code true} if there was no failure during the aggregation request processing on the specified cluster node.
     *
     * @param node Cluster node (must be one of the {@link #nodes()}).
     *
     * @return {@code true} if there was no communication failure with the specified cluster node.
     */
    boolean isSuccess(ClusterNode node);

    /**
     * Returns the aggregation results.
     *
     * @return Aggregation results.
     */
    Collection<T> results();

    /**
     * Returns the aggregation results as {@link Stream}.
     *
     * @return Stream of aggregation results.
     */
    Stream<T> stream();

    /**
     * Returns a map of cluster nodes and results that were successfully received from these nodes. Returns an empty map if there were no
     * successful results.
     *
     * @return Map of results.
     *
     * @see #resultOf(ClusterNode)
     */
    Map<ClusterNode, T> resultsByNode();

    /**
     * Returns the result for the specified cluster node or {@code null} if there was no result from this node.
     *
     * @param node Cluster node (must be one of the {@link #nodes()}).
     *
     * @return Result.
     *
     * @see #results()
     */
    T resultOf(ClusterNode node);
}
