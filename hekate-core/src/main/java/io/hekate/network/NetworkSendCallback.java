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

package io.hekate.network;

/**
 * Callback for getting results of an asynchronous messages sending operation in {@link NetworkEndpoint}.
 *
 * <p>
 * Note that this callback is called on the {@link NetworkEndpoint}'s NIO thread. It must be executed as fast as possible in order to
 * prevent blocking other endpoints that can be associated with the same thread. In case of long/heavy computations consider scheduling
 * such tasks to another thread.
 * </p>
 *
 * @param <T> {@link NetworkEndpoint}'s base message type.
 */
@FunctionalInterface
public interface NetworkSendCallback<T> {
    /**
     * Called when {@link NetworkEndpoint#send(Object, NetworkSendCallback) message sending} completes either successfully or with an
     * error.
     *
     * @param msg Submitted message.
     * @param error Error if happened or {@code null} if operation was successful.
     */
    void onComplete(T msg, Throwable error);
}
