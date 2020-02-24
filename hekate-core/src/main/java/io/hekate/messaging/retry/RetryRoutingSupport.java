/*
 * Copyright 2020 The Hekate Project
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
 * Template interface for policies that can re-route operations upon retry.
 *
 * @param <P> Policy type.
 */
public interface RetryRoutingSupport<P extends RetryRoutingSupport<P>> {
    /**
     * Routing policy in case of a retry.
     *
     * @param policy Policy.
     *
     * @return This instance.
     */
    P route(RetryRoutingPolicy policy);

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#RE_ROUTE} policy.
     *
     * @return This instance.
     */
    default P alwaysReRoute() {
        return route(RetryRoutingPolicy.RE_ROUTE);
    }

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#RETRY_SAME_NODE} policy.
     *
     * @return This instance.
     */
    default P alwaysTrySameNode() {
        return route(RetryRoutingPolicy.RETRY_SAME_NODE);
    }

    /**
     * Instructs to apply the {@link RetryRoutingPolicy#PREFER_SAME_NODE} policy.
     *
     * @return This instance.
     */
    default P preferSameNode() {
        return route(RetryRoutingPolicy.PREFER_SAME_NODE);
    }
}
