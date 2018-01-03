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

package io.hekate.messaging.broadcast;

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingFuture;

/**
 * Asynchronous result of {@link MessagingChannel#broadcast(Object) broadcast(...)} operation.
 *
 * @param <T> Base type of a broadcast message.
 *
 * @see MessagingChannel#broadcast(Object)
 * @see BroadcastResult
 */
public class BroadcastFuture<T> extends MessagingFuture<BroadcastResult<T>> {
    // No-op.
}
