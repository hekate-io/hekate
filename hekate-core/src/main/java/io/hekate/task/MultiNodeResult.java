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
 * Result of task execution on multiple nodes.
 *
 * <p>
 * This interface represents the result of a distributed task execution via {@link TaskService#broadcast(RunnableTask) broadcast(...)} or
 * {@link TaskService#aggregate(CallableTask) aggregate(...)} method. It contains information about {@link #getNodes() nodes} that
 * participated in the task execution and execution {@link #getResult(ClusterNode) result}/{@link #getError(ClusterNode) error} of each
 * node.
 * </p>
 *
 * @param <T> Result type.
 *
 * @see TaskService#broadcast(RunnableTask)
 * @see TaskService#aggregate(CallableTask)
 */
public interface MultiNodeResult<T> {
    /**
     * Returns the set of nodes that participated in the task execution. Nodes are selected based on {@link TaskService} filtering rules
     * (see {@link TaskService#filter(ClusterNodeFilter)}).
     *
     * @return Set of nodes or empty set of no nodes were selected for task execution.
     */
    Set<ClusterNode> getNodes();

    /**
     * Returns {@code true} if task was successfully executed by all nodes an there were no failures. If this method returns
     * {@code false} then individual node failures can be analyzed via {@link #getError(ClusterNode)} method.
     *
     * @return {@code true} if task was successfully executed by all nodes without any errors.
     */
    boolean isSuccess();

    /**
     * Returns {@code true} if the task was successfully executed by the specified cluster without an error. If this method returns
     * {@code false} then error cause can be analyzed via {@link #getError(ClusterNode)} method.
     *
     * <p>
     * Note that the specified cluster node must be a member of {@link #getNodes()}.
     * </p>
     *
     * @param node Cluster node (must be one of {@link #getNodes()}).
     *
     * @return {@code true} if the task was successfully executed by the specified cluster without an error.
     */
    boolean isSuccess(ClusterNode node);

    /**
     * Returns the task execution result for the specified cluster node. Returns {@code null} if task execution failed at the specified
     * node (see {@link #isSuccess(ClusterNode)}).
     *
     * @param node Cluster node (must be one of {@link #getNodes()}).
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
     * @param node Cluster node (must be one of {@link #getNodes()}).
     *
     * @return Error or {@code null} if task was executed successfully by the specified cluster node.
     */
    Throwable getError(ClusterNode node);

    /**
     * Returns a map of successful nodes with their corresponding results.
     *
     * @return Collection of all successfully executed tasks.
     *
     * @see #getErrors()
     */
    Map<ClusterNode, T> getResults();

    /**
     * Returns a map of failed nodes with their corresponding errors.
     *
     * @return Map of task execution errors or an empty map if task was executed successfully.
     *
     * @see #getResults()
     */
    Map<ClusterNode, Throwable> getErrors();
}
