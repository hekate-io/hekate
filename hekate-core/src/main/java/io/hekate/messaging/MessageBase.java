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

package io.hekate.messaging;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;

/**
 * Base interface for messages.
 *
 * @param <T> Payload type.
 */
public interface MessageBase<T> {
    /**
     * Returns the payload of this message.
     *
     * @return Payload.
     */
    T payload();

    /**
     * Casts the payload of this message to the specified type.
     *
     * @param type Payload type.
     * @param <P> Payload type.
     *
     * @return Payload.
     *
     * @throws ClassCastException If payload can't be cast to the specified type.
     */
    <P extends T> P payload(Class<P> type);

    /**
     * Returns {@code true} if this message has a {@link #payload() payload} of the specified type.
     *
     * @param type Payload type.
     *
     * @return {@code true} if this message has a {@link #payload() payload} of the specified type.
     */
    boolean is(Class<? extends T> type);

    /**
     * Returns the messaging endpoint of this message..
     *
     * @return Messaging endpoint.
     */
    MessagingEndpoint<T> endpoint();

    /**
     * Returns the messaging channel of this message.
     *
     * @return Messaging channel.
     */
    default MessagingChannel<T> channel() {
        return endpoint().channel();
    }

    /**
     * Returns the universally unique identifier of the remote cluster node.
     *
     * @return Universally unique identifier of the remote cluster node.
     *
     * @see ClusterNode#id()
     */
    default ClusterAddress from() {
        return endpoint().remoteAddress();
    }
}
