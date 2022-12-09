/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.messaging.intercept;

import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Request;
import io.hekate.messaging.operation.Send;
import io.hekate.messaging.operation.Subscribe;

/**
 * Type of an outbound message.
 *
 * @see MessageInterceptor
 */
public enum OutboundType {
    /** Message is submitted as a {@link Request} operation. */
    REQUEST,

    /** Message is submitted as a {@link Subscribe} operation. */
    SUBSCRIBE,

    /**
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(AckMode)} set to {@link AckMode#REQUIRED}.
     */
    SEND_WITH_ACK,

    /**
     * Message is submitted as a {@link Send} operation with {@link Send#withAckMode(AckMode)} set to {@link AckMode#NOT_NEEDED}.
     */
    SEND_NO_ACK
}
