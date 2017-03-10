/*
 * Copyright 2017 The Hekate Project
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

import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;

/**
 * Reply for {@link MessagingChannel#request(Object, RequestCallback) request(...)} operation.
 *
 * @param <T> Payload type.
 *
 * @see RequestCallback#onComplete(Throwable, Reply)
 */
public interface Reply<T> extends Message<T> {
    /**
     * Returns the request.
     *
     * @return Request.
     */
    T getRequest();

    /**
     * Returns {@code true} if this message is a partial response that was produced by the {@link #replyPartial(Object, SendCallback)}
     * method. This flag can be used to check if there is more data on the messaging stream or if this is the final response.
     *
     * @return {@code true} if this message is a partial response.
     */
    boolean isPartial();
}
