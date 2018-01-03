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
 * Failover context.
 *
 * <p>
 * This interface provides failure details to a {@link FailoverPolicy}. Policy can use this information in order to decide on
 * whether a failed operation should be {@link #retry() retried} or should {@link #fail() fail} with an error.
 * </p>
 *
 * <p>
 * For more details about failover please see the documentation of {@link FailoverPolicy} interface.
 * </p>
 *
 * @see FailoverPolicy
 */
public interface FailoverContext extends FailureInfo {
    /**
     * Resolve to fail.
     *
     * @return Failure resolution.
     */
    FailureResolution fail();

    /**
     * Resolve to retry.
     *
     * @return Failure resolution.
     */
    FailureResolution retry();
}
