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

package io.hekate.task;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import java.util.Map;
import java.util.Set;

/**
 * Result of the task execution on multiple nodes.
 *
 * <p>
 * This interface represents the result of executing a distributed task via a {@link TaskService#broadcast(RunnableTask) broadcast(...)} or
 * {@link TaskService#aggregate(CallableTask) aggregate(...)} method. It contains information about the {@link #getNodes() nodes} involved
 * in the task execution, and the {@link #getResult(ClusterNode) result}/{@link #getError(ClusterNode) error} of execution of each node.
 * </p>
 *
 * @param <T> Result type.
 *
 * @see TaskService#broadcast(RunnableTask)
 * @see TaskService#aggregate(CallableTask)
 */
public interface MultiNodeResult<T> {
    /**
     * Returns the set of nodes that participated in the task execution. The nodes are selected based on the filtering rules that were
     * applied to the {@link TaskService} (see {@link TaskService#filter(ClusterNodeFilter)}).
     *
     * @return Set of nodes or an empty set if no nodes were selected for task execution.
     */
    Set<ClusterNode> getNodes();

    /**
     * Returns {@code true} if the task was successfully executed by all nodes and there were no failures. If this method returns
     * {@code false}, then individual node failures can be analyzed using the {@link #getError(ClusterNode)} method.
     *
     * @return {@code true} if the task was successfully executed by all nodes and there were no failures.
     */
    boolean isSuccess();

    /**
     * Returns {@code true} if the task was successfully executed by the specified node and there were no failures. If this method returns
     * {@code false}, then the failure of the specified node can be analyzed using the {@link #getError (ClusterNode)} method.
     *
     * <p>
     * Note that the specified cluster node must be a member of {@link #getNodes()}.
     * </p>
     *
     * @param node Cluster node (must be one of the {@link #getNodes()}).
     *
     * @return {@code true} if the task was successfully executed by the specified node.
     */
    boolean isSuccess(ClusterNode node);

    /**
     * Returns the task execution result for the specified cluster node. Returns {@code null} if task execution failed on the specified
     * node (see {@link #isSuccess(ClusterNode)}).
     *
     * @param node Cluster node (must be one of the {@link #getNodes()}).
     *
     * @return Task execution result for the specified node or {@code null} if result is not available due to an error.
     *
     * @see #getError(ClusterNode)
     */
    T getResult(ClusterNode node);

    /**
     * Returns the task execution error for the specified cluster node. Returns {@code null} if task was executed successfully by that
     * node (see {@link #isSuccess(ClusterNode)}).
     *
     * @param node Cluster node (must be one of the {@link #getNodes()}).
     *
     * @return Error or {@code null} if task was executed successfully by the specified cluster node.
     */
    Throwable getError(ClusterNode node);

    /**
     * Returns the map of the nodes that successfully executed this task and their respective results.
     *
     * @return Map of the nodes that successfully executed this task and their respective results.
     *
     * @see #getErrors()
     */
    Map<ClusterNode, T> getResults();

    /**
     * Returns the map of the failed nodes with the corresponding errors.
     *
     * @return Map of the failed nodes with the corresponding errors.
     *
     * @see #getResults()
     */
    Map<ClusterNode, Throwable> getErrors();
}
