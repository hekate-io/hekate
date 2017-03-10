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

package io.hekate.core.service;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;

/**
 * Context for {@link InitializingService}.
 */
public interface InitializationContext {
    /**
     * Returns the cluster name.
     *
     * @return Cluster name.
     */
    String getClusterName();

    /**
     * Returns the current state of {@link Hekate} instance that this service belongs to.
     *
     * @return State.
     */
    Hekate.State getState();

    /**
     * Returns cluster context.
     *
     * @return Cluster context.
     */
    ClusterContext getCluster();

    /**
     * Returns the cluster node.
     *
     * @return Cluster node.
     */
    ClusterNode getNode();

    /**
     * Returns the {@link Hekate} instance that this service belongs to.
     *
     * @return {@link Hekate} instance that this service belongs to.
     */
    Hekate getHekate();

    /**
     * Instructs local node to asynchronously leave and rejoin to the cluster.
     */
    void rejoin();

    /**
     * Triggers asynchronous {@link Hekate#terminateAsync() termination} of the local node.
     */
    void terminate();

    /**
     * Triggers asynchronous {@link Hekate#terminateAsync() termination} of the local node with the specified error..
     *
     * @param e Cause of termination.
     */
    void terminate(Throwable e);
}
