/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.cluster.ClusterAddress;
import io.hekate.messaging.MessagingChannel;

/**
 * Server's inbound message context.
 *
 * @param <T> Message type.
 */
public interface ServerInboundContext<T> {
    /**
     * Type of a send operation.
     *
     * @return Type of a send operation.
     */
    OutboundType type();

    /**
     * Returns the inbound message.
     *
     * @return Message.
     */
    T payload();

    /**
     * Returns the channel name (see {@link MessagingChannel#name()}).
     *
     * @return Channel name.
     */
    String channelName();

    /**
     * Address of a remote node.
     *
     * @return Address of a remote node.
     */
    ClusterAddress from();

    /**
     * Returns the attribute for the specified name.
     *
     * @param name Name.
     *
     * @return Value or {@code null} if there is no such attribute.
     *
     * @see ServerReceiveContext#setAttribute(String, Object)
     */
    Object getAttribute(String name);
}
