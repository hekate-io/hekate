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

package io.hekate.election;

import io.hekate.cluster.ClusterNode;

/**
 * Follower state context for {@link Candidate}.
 *
 * <p>
 * For more details about the leader election process please see the documentation of {@link ElectionService} interface.
 * </p>
 *
 * @see Candidate#becomeFollower(FollowerContext)
 */
public interface FollowerContext {
    /**
     * Returns a node that is currently holding the leadership.
     *
     * @return Leader node.
     */
    ClusterNode getLeader();

    /**
     * Returns the local node where the {@link Candidate} is running.
     *
     * @return Local node.
     */
    ClusterNode getLocalNode();

    /**
     * Registers the leader change event listener.
     *
     * <p>
     * The specified listener will be notified every time when some other node becomes a new leader.
     * </p>
     *
     * <p>
     * This listener will be kept registered only while {@link Candidate candidate} stays in the follower state. Listener
     * will be automatically unregistered when {@link Candidate candidate} switches to the leader state.
     * </p>
     *
     * @param listener Listener.
     */
    void addLeaderChangeListener(LeaderChangeListener listener);

    /**
     * Unregisters the specified listener if it was previously registered via {@link #addLeaderChangeListener(LeaderChangeListener)}.
     *
     * @param listener Listener.
     *
     * @return {@code true} if listener was removed.
     */
    boolean removeLeaderChangeListener(LeaderChangeListener listener);
}
