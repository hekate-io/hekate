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

import io.hekate.messaging.MessageMetaData;

/**
 * Server's outbound message context.
 *
 * @param <T> Message type.
 *
 * @see ServerMessageInterceptor#interceptServerSend(ServerSendContext)
 */
public interface ServerSendContext<T> {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();

    /**
     * Returns the outbound message.
     *
     * @return Outbound message.
     */
    T payload();

    /**
     * Returns the inbound context.
     *
     * @return Inbound context.
     */
    ServerInboundContext<T> inboundContext();

    /**
     * Overrides the message to be sent with the specified one.
     *
     * @param msg New message that should be sent instead of the original one.
     */
    void overrideMessage(T msg);

    /**
     * Returns {@code true} if this message has meta-data.
     *
     * @return {@code true} if this message has meta-data.
     *
     * @see #metaData()
     */
    boolean hasMetaData();

    /**
     * Returns the message's meta-data.
     *
     * @return Message meta-data.
     *
     * @see #hasMetaData()
     */
    MessageMetaData metaData();
}
