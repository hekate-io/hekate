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

package io.hekate.cluster.split;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.Hekate;

/**
 * Possible options for <a href="https://en.wikipedia.org/wiki/Split-brain_(computing)" target="_blank">Split-brain</a> resolution.
 *
 * @see ClusterServiceFactory#setSplitBrainAction(SplitBrainAction)
 */
public enum SplitBrainAction {
    /**
     * Restart node and rejoin it to the cluster. All of node service will be unavailable and will throw illegal state
     * errors while node is rejoining.
     *
     * <p>
     * Rejoined node will have completely new identity and will be visible to other cluster members as a completely new node. After rejoin
     * {@link Hekate#localNode()} will have new {@link ClusterNode#id() identifier} and {@link ClusterNode#joinOrder() join order
     * index}.
     * </p>
     */
    REJOIN,

    /**
     * Node should be immediately {@link Hekate#terminate() terminated}.
     */
    TERMINATE,

    /**
     * Kills the JVM via {@link System#exit(int)} with exit code {@code 250}. This policy is expected to be used with some sort of a shell
     * script or a process monitoring daemon that is capable of restarting processes.
     */
    KILL_JVM
}
