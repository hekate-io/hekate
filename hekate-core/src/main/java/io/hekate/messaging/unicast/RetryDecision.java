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

package io.hekate.messaging.unicast;

import io.hekate.failover.FailoverPolicy;

/**
 * Result of {@link RequestRetryCondition}.
 *
 * @see RequestRetryCondition#accept(Throwable, Response)
 */
public enum RetryDecision {
    /**
     * Signals that operation result was accepted and operation should be completed. Note that {@link FailoverPolicy} will not be applied
     * in such case even if operation ended up with an error.
     */
    DONE,

    /**
     * Signals that operation should fail with a {@link RejectedReplyException} and a {@link FailoverPolicy} should be applied. If failover
     * policy is not configured for the channel then the operation will be completed with {@link RejectedReplyException}.
     */
    RETRY,

    /**
     * Signals that the default handling logic should be applied, i.e. if operation failed with an error then {@link FailoverPolicy} should
     * be applied (if configured for the channel); otherwise the operation should be completed with whatever result is available.
     */
    USE_DEFAULTS
}
