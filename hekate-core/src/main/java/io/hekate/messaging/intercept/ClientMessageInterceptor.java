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

import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.messaging.operation.AckMode;
import io.hekate.messaging.operation.Broadcast;
import io.hekate.messaging.operation.Send;

/**
 * Client-side message interceptor.
 *
 * <p>
 * Please see the documentation of {@link MessageInterceptor} for details of message interception logic.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 */
public interface ClientMessageInterceptor<T> extends MessageInterceptor {
    /**
     * Intercepts a message right before sending it to a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} after the target server had been selected by the {@link LoadBalancer} for
     * the specified outbound message. Implementations of this method can {@link ClientSendContext#overrideMessage(Object) override}
     * the message with another message based on some criteria of the {@link ClientSendContext}.
     * </p>
     *
     * @param ctx Context.
     */
    default void interceptClientSend(ClientSendContext<T> ctx) {
        // No-op.
    }

    /**
     * Intercepts a response from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a response from a server for a message of
     * {@link OutboundType#REQUEST} or {@link OutboundType#SUBSCRIBE} type.
     * Implementations of this method can {@link ClientSendContext#overrideMessage(Object) override} the message with another message based
     * on some criteria of the {@link ClientReceiveContext}.
     * </p>
     *
     * @param ctx Response context.
     */
    default void interceptClientReceiveResponse(ClientReceiveContext<T> ctx) {
        // No-op.
    }

    /**
     * Notifies on an acknowledgement from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives an acknowledgement from a server for a message of
     * {@link OutboundType#SEND_WITH_ACK} type.
     * </p>
     *
     * @param ctx Context of a sent message (same object as in {@link #interceptClientSend(ClientSendContext)}).
     *
     * @see Send#withAckMode(AckMode)
     * @see Broadcast#withAckMode(AckMode)
     */
    default void interceptClientReceiveAck(ClientOutboundContext<T> ctx) {
        // No-op.
    }

    /**
     * Notifies on a failure to receive a response from server.
     *
     * @param ctx Context of a message (same object as in {@link #interceptClientSend(ClientSendContext)}).
     * @param err Error
     */
    default void interceptClientReceiveError(ClientOutboundContext<T> ctx, Throwable err) {
        // No-op.
    }
}
