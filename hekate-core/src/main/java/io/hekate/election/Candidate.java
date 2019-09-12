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

package io.hekate.election;

/**
 * Leader election candidate.
 *
 * <p>
 * Implementations of this interface can be registered within the {@link ElectionService} in order to participate in the leader
 * election process. If this candidate wins then its {@link #becomeLeader(LeaderContext)} method will be called. If some other candidate
 * wins then this candidate will be notified via {@link #becomeFollower(FollowerContext)} method. If later on the existing leader leaves
 * the cluster or {@link LeaderContext#yieldLeadership() yeilds leadship} then this candidate will try again to become a new leader. If
 * it finally succeeds then {@link #becomeLeader(LeaderContext)} method will be called.
 * </p>
 *
 * <p>
 * For more details about the leader election process please see the documentation of {@link ElectionService} interface.
 * </p>
 *
 * @see ElectionServiceFactory#withCandidate(CandidateConfig)
 */
public interface Candidate {
    /**
     * Gets called when this candidate becomes a group leader.
     *
     * <p>
     * <b>IMPORTANT:</b> Implementations of this method should not block the calling thread for a long time and should execute all long
     * running computations asynchronously.
     * </p>
     *
     * @param ctx Leader context.
     */
    void becomeLeader(LeaderContext ctx);

    /**
     * Gets called when this candidate couldn't win elections and switched to the follower state. Information about the current leader can
     * be obtained via {@link FollowerContext#leader()}.
     *
     * <p>
     * <b>IMPORTANT:</b> Implementations of this method should not block the calling thread for a long time and should execute all long
     * running computations asynchronously.
     * </p>
     *
     * @param ctx Follower context.
     */
    void becomeFollower(FollowerContext ctx);

    /**
     * Gets called when candidate must be terminated because of the {@link ElectionService} being stopped.
     *
     * <p>
     * Note that this method will NOT be called if this candidate never reached any of the Leader/Follower state (i.e. if {@link
     * #becomeLeader(LeaderContext)} or {@link #becomeFollower(FollowerContext)} were never called).
     * </p>
     */
    void terminate();
}
