/*
 * Copyright 2020 The Hekate Project
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
import java.util.Optional;

/**
 * Client's receive context.
 *
 * @param <T> Message type.
 *
 * @see ClientMessageInterceptor#interceptClientReceiveResponse(ClientReceiveContext)
 */
public interface ClientReceiveContext<T> {
    /**
     * Type of this response.
     *
     * @return Type of this response.
     */
    InboundType type();

    /**
     * Returns the inbound message.
     *
     * @return Message.
     */
    T payload();

    /**
     * Returns the outbound context.
     *
     * @return Outbound context.
     */
    ClientOutboundContext<T> outboundContext();

    /**
     * Overrides the received message with the specified one.
     *
     * @param msg New message that should replace the received one.
     */
    void overrideMessage(T msg);

    /**
     * Reads the message's meta-data.
     *
     * @return Message's meta-data.
     */
    Optional<MessageMetaData> readMetaData();
}
