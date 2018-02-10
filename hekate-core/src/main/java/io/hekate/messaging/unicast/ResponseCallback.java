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

import io.hekate.messaging.MessagingChannel;

/**
 * Callback for {@link MessagingChannel#request(Object, ResponseCallback) request(...)} operation.
 *
 * @param <T> Base type of request message.
 *
 * @see MessagingChannel#request(Object, ResponseCallback)
 */
@FunctionalInterface
public interface ResponseCallback<T> {
    /**
     * Called when a request operation gets completed either successfully or with an error.
     *
     * <p>
     * If operation completes with an error then {@code err} parameter will hold the error cause and {@code reply} parameter will be
     * {@code null}. If operation completes successfully then {@code err} parameter will be {@code null} and {@code reply} parameter
     * will hold the reply message.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param rsp Response ({@code null} if operation failed).
     */
    void onComplete(Throwable err, Response<T> rsp);

    /**
     * Called upon a request operation completion (either successfully or with an error) in order decide on whether the operation outcome
     * can be accepted by this callback.
     *
     * <p>
     * For more details about possible decisions please see the documentation of {@link ReplyDecision} enum values.
     * </p>
     *
     * <p>
     * Default implementation of this method always returns {@link ReplyDecision#DEFAULT}.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param reply Response ({@code null} if operation failed).
     *
     * @return Decision.
     */
    default ReplyDecision accept(Throwable err, Response<T> reply) {
        return ReplyDecision.DEFAULT;
    }
}
