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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateSupport;

/**
 * Leader state context for {@link Candidate}.
 *
 * <p>
 * For more details about the leader election process please see the documentation of {@link ElectionService} interface.
 * </p>
 *
 * @see Candidate#becomeLeader(LeaderContext)
 */
public interface LeaderContext extends HekateSupport {
    /**
     * Returns the local node where the {@link Candidate} is running.
     *
     * @return Local node.
     */
    ClusterNode localNode();

    /**
     * Asynchronously yields leadership and gives other {@link Candidate candidates} a chance to become a leader.
     *
     * <p>
     * Note that this method is asynchronous and all of its activities are preformed on a background thread. Once operation completes
     * then {@link Candidate#becomeFollower(FollowerContext)} or {@link Candidate#becomeLeader(LeaderContext)}
     * method will be called depending on new leader election results.
     * </p>
     */
    void yieldLeadership();
}
