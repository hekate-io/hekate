/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.failover;

/**
 * Failure resolution.
 *
 * <p>
 * This interface represents the result of {@link FailoverPolicy} invocation and contains instructions on how a
 * particular failure should be handled. Instances of this interface can be constructed via {@link FailoverContext#fail()} and {@link
 * FailoverContext#retry()} methods.
 * </p>
 *
 * <p>
 * For more details about failover please see the documentation of {@link FailoverPolicy} interface.
 * </p>
 *
 * @see FailoverPolicy
 */
public interface FailureResolution {
    /**
     * Returns {@code true} if {@link FailoverPolicy} decided to retry the failed operation.
     *
     * <p>
     * This flag cant't be changed explicitly and returns {@code true} or {@code false} depending on whether this instance was
     * constructed via {@link FailoverContext#retry()} or {@link FailoverContext#fail()}.
     * </p>
     *
     * @return {@code true} if {@link FailoverPolicy} decided to retry the failed operation.
     */
    boolean isRetry();

    /**
     * Instructs to retry the operation after the specified delay (in milliseconds). Using zero (default)
     * or negative value instructs to retry the operation without any delay.
     *
     * <p>
     * <b>Note:</b> calling this method has no effect if {@link #isRetry()} returns {@code false}.
     * </p>
     *
     * @param delay Delay in milliseconds.
     *
     * @return This instance.
     */
    FailureResolution withDelay(long delay);

    /**
     * Instructs to apply the {@link FailoverRoutingPolicy#RE_ROUTE} policy.
     *
     * @return This instance.
     */
    FailureResolution withReRoute();

    /**
     * Instructs to apply the {@link FailoverRoutingPolicy#RETRY_SAME_NODE} policy.
     *
     * @return This instance.
     */
    FailureResolution withSameNode();

    /**
     * Instructs to apply the {@link FailoverRoutingPolicy#PREFER_SAME_NODE} policy.
     *
     * @return This instance.
     */
    FailureResolution withSameNodeIfExists();

    /**
     * Instructs to apply the specified routing policy.
     *
     * @param policy Routing policy.
     *
     * @return This instance.
     */
    FailureResolution withRoutingPolicy(FailoverRoutingPolicy policy);

    /**
     * Returns the delay that was set via {@link #withDelay(long)}.
     *
     * @return Delay in milliseconds.
     *
     * @see #withDelay(long)
     */
    long delay();

    /**
     * Returns the routing policy that should be applied during failover.
     *
     * @return Routing policy.
     *
     * @see #withSameNode()
     * @see #withSameNodeIfExists()
     * @see #withReRoute()
     */
    FailoverRoutingPolicy routing();
}
