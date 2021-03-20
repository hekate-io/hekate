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

package io.hekate.messaging.retry;

import io.hekate.messaging.operation.Response;

/**
 * Predicate to test if particular response should be retried.
 *
 * @param <T> Response type.
 *
 * @see RetryResponseSupport
 */
@FunctionalInterface
public interface RetryResponsePredicate<T> {
    /**
     * Called upon a request operation completion in order decide on whether the result is acceptable or should the operation be retried.
     *
     * <ul>
     * <li>{@code true} - Signals that operation should be retried</li>
     * <li>{@code false} - signals that response is accepted and operation should be completed</li>
     * </ul>
     *
     * @param rsp Response.
     *
     * @return Decision.
     */
    boolean shouldRetry(Response<T> rsp);
}
