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

import io.hekate.messaging.Message;
import io.hekate.messaging.operation.SendCallback;

/**
 * Type of an inbound message.
 *
 * @see MessageInterceptor
 */
public enum InboundType {
    /** Chunk of a bigger response (see {@link Message#partialReply(Object, SendCallback)}). */
    RESPONSE_CHUNK,

    /** Final response (See {@link Message#reply(Object, SendCallback)}). */
    FINAL_RESPONSE
}
