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

/**
 * Message receiver.
 *
 * <p>
 * This interface represents a receiver that can be registered to the {@link MessagingChannel}
 * (via {@link MessagingChannelConfig#setReceiver(MessageReceiver)} method) in order to receive messages from remote nodes.
 * </p>
 *
 * <p>
 * For more details about messaging please see the documentation of {@link MessagingService} interface.
 * </p>
 *
 * @param <T> Base type of messages that can be handled by this receiver.
 *
 * @see MessagingChannelConfig#setReceiver(MessageReceiver)
 * @see MessagingService
 */
@FunctionalInterface
public interface MessageReceiver<T> {
    /**
     * Called when a new message is received by this channel.
     *
     * @param msg Message.
     */
    void receive(Message<T> msg);

    /**
     * Called when a remote node connects to this channel.
     *
     * @param endpoint Connected endpoint.
     */
    default void onConnect(MessagingEndpoint<T> endpoint) {
        // No-op.
    }

    /**
     * Called when a remote node disconnects from this channel.
     *
     * @param endpoint Disconnected endpoint.
     */
    default void onDisconnect(MessagingEndpoint<T> endpoint) {
        // No-op.
    }
}
