/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.messaging.operation;

/**
 * Configurer of {@link RequestRetryPolicy}.
 *
 * @param <T> Message type.
 *
 * @see Request#withRetry(RequestRetryConfigurer)
 * @see Subscribe#withRetry(RequestRetryConfigurer)
 */
@FunctionalInterface
public interface RequestRetryConfigurer<T> {
    /**
     * Configures the retry policy.
     *
     * @param retry Retry policy.
     */
    void configure(RequestRetryPolicy<T> retry);
}
