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

package io.hekate.coordinate;

import io.hekate.cluster.ClusterNode;

/**
 * Member of a coordination process.
 *
 * @see CoordinationContext#members()
 */
public interface CoordinationMember {
    /**
     * Returns {@code true} if this member was selected to be the coordinator.
     *
     * @return {@code true} if this member was selected to be the coordinator.
     */
    boolean isCoordinator();

    /**
     * Returns the cluster node of this member.
     *
     * @return Cluster node.
     */
    ClusterNode node();

    /**
     * Asynchronously send the specified request to this member.
     *
     * @param request Request.
     * @param callback Callback to be notified upon member response.
     *
     * @see CoordinationHandler#process(CoordinationRequest, CoordinationContext)
     */
    void request(Object request, CoordinationRequestCallback callback);
}
