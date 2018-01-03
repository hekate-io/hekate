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
 * <span class="startHere">&laquo; start here</span>Failover policy.
 *
 * <p>
 * This interface represent a failure resolution policy that can be consulted in order to decide on whether a failed operation can be
 * retried or it should fail permanently. Policy can use {@link FailoverContext} in order to get all information about the failed
 * operation and return a {@link FailureResolution} instance as follows:
 * </p>
 * <ul>
 * <li>if decision is to retry then {@link FailoverContext#retry()} method must be used to construct a new {@link FailureResolution}
 * instance</li>
 * <li>if decision is to fail then {@link FailoverContext#fail()} method must be used to construct a new {@link FailureResolution}
 * instance</li>
 * </ul>
 *
 * <p>
 * It is possible to customize the retry behavior by setting a number of options on a {@link FailureResolution} instance. For example
 * it is possible to specify the delay before retrying.
 * </p>
 *
 * <p>
 * Below is the example of a simple policy implementation that performs a limited number of reties with a fixed delay between attempts.
 * ${source: failover/ExampleFailoverPolicy.java#example}
 * </p>
 */
@FunctionalInterface
public interface FailoverPolicy {
    /**
     * Applies this failover policy to the provided failover context.
     *
     * @param context Failure context.
     *
     * @return Failure resolution.
     */
    FailureResolution apply(FailoverContext context);

    /**
     * Returns the failover policy that always {@link FailoverContext#fail() fails}.
     *
     * @return Failover policy.
     */
    static FailoverPolicy alwaysFail() {
        return FailoverContext::fail;
    }
}
