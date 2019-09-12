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

package io.hekate.messaging.retry;

/**
 * Routing behavior in case of a retry action.
 */
public enum RetryRoutingPolicy {
    /**
     * Do not try to re-route and always use the same {@link FailedAttempt#lastTriedNode() node}.
     */
    RETRY_SAME_NODE,

    /**
     * If {@link FailedAttempt#lastTriedNode() failed node} is still within the cluster topology then try using it. If failed node left the
     * cluster topology then preform re-routing.
     */
    PREFER_SAME_NODE,

    /** Always preform re-routing. */
    RE_ROUTE;

    /**
     * Returns the default policy.
     *
     * @return Default policy.
     */
    public static RetryRoutingPolicy defaultPolicy() {
        return PREFER_SAME_NODE;
    }
}
