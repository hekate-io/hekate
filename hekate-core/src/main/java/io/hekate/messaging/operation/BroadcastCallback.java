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

package io.hekate.messaging.operation;

/**
 * Callback for a {@link Broadcast} operation.
 *
 * @param <T> Base type of a messages.
 *
 * @see Broadcast#submit(BroadcastCallback)
 * @see BroadcastResult
 */
@FunctionalInterface
public interface BroadcastCallback<T> {
    /**
     * Called when the broadcast operation gets completed either successfully or with an error.
     *
     * <p>
     * If operation completes with a fatal error (f.e channel is closed) then {@code err} parameter will hold the error cause and
     * {@code result} parameter will be {@code null}. Otherwise {@code err} parameter will be {@code null} and {@code result} parameter
     * will hold the broadcast operation result.
     * </p>
     *
     * @param err Error ({@code null} if operation was successful).
     * @param result Broadcast operation result ({@code null} if operation failed).
     */
    void onComplete(Throwable err, BroadcastResult<T> result);
}
