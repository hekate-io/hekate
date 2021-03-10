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

import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingChannel;

/**
 * Server-side message interceptor.
 *
 * <p>
 * Please see the documentation of {@link MessageInterceptor} for details of message interception logic.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 */
public interface ServerMessageInterceptor<T> extends MessageInterceptor {
    /**
     * Intercepts an incoming message from a remote client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a message from a client, right before invoking
     * the {@link MessageReceiver}. Implementations of this method can {@link ServerReceiveContext#overrideMessage(Object) override}
     * the message with another message based on some criteria of the {@link ServerReceiveContext}.
     * </p>
     *
     * @param ctx Context.
     */
    default void interceptServerReceive(ServerReceiveContext<T> ctx) {
        // No-op.
    }

    /**
     * Notifies on server completes processing of a message.
     *
     * <p>
     * This method gets called right after the server's {@link MessageReceiver} completes processing of a message.
     * </p>
     *
     * @param ctx Context of a request message (same object as in {@link #interceptServerReceive(ServerReceiveContext)}).
     */
    default void interceptServerReceiveComplete(ServerInboundContext<T> ctx) {
        // No-op.
    }

    /**
     * Intercepts a response right before sending it back to a client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} right before sending a response message to a client.
     * Implementations of this method can {@link ServerReceiveContext#overrideMessage(Object) override} the message with another message
     * based on some criteria of the {@link ServerSendContext}.
     * </p>
     *
     * @param ctx Response context.
     */
    default void interceptServerSend(ServerSendContext<T> ctx) {
        // No-op.
    }
}
