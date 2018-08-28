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
import io.hekate.messaging.MessagingFuture;
import java.util.List;

/**
 * Asynchronous result of {@link MessagingChannel#request(Object) request(...)} operation.
 *
 * @param <T> Base type of request message.
 *
 * @see MessagingChannel#request(Object)
 */
public class SubscribeFuture<T> extends MessagingFuture<List<T>> {
    // No-op.
}
