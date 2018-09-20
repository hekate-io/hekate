/*
 * Copyright 2018 The Hekate Project
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
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.loadbalance.LoadBalancer;
import java.util.List;

/**
 * Message interceptor.
 *
 * <p>
 * Instances of this interface can be {@link MessagingChannelConfig#setInterceptors(List) registered} to a messaging channel in
 * order to intercept and transform messages.
 * </p>
 *
 * <pre>{@code
 *          Client Node                                         Server Node
 * +-----------------------------+       request       +-----------------------------+
 * | interceptClientSend(...)    + ------------------> + interceptServerReceive(...) |
 * +-----------------------------+                     +--------------+--------------+
 *                                                             process request
 * +-----------------------------+       response      +--------------v--------------+
 * | interceptClientReceive(...) + <------------------ + interceptServerSend(...)    |
 * +-----------------------------+                     +-----------------------------+
 * }</pre>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 *
 * @see MessagingChannelConfig#setInterceptors(List)
 */
public interface MessageInterceptor<T> {
    /**
     * Intercepts a message right before sending it to a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} after the target server had been selected by the {@link LoadBalancer} for the
     * specified outbound message. Implementations of this method can decide to transform the message into another message based on some
     * criteria of the {@link ClientSendContext}.
     * </p>
     *
     * @param msg Message.
     * @param sndCtx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #interceptClientReceive(Object, ResponseContext, ClientSendContext)
     */
    default T interceptClientSend(T msg, ClientSendContext sndCtx) {
        return null;
    }

    /**
     * Intercepts a response from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a response from a server for a message of
     * {@link OutboundType#REQUEST} or {@link OutboundType#SUBSCRIBE} type. Implementations of this method can decide to transform the
     * message into another message based on some criteria of the {@link ClientSendContext}.
     * </p>
     *
     * @param rsp Response.
     * @param rspCtx Response context.
     * @param sndCtx Context of a sent message (same object as in {@link #interceptClientSend(Object, ClientSendContext)}).
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #interceptClientSend(Object, ClientSendContext)
     */
    default T interceptClientReceive(T rsp, ResponseContext rspCtx, ClientSendContext sndCtx) {
        return null;
    }

    /**
     * Intercepts a void response from a server.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives an acknowledgement from a server for a message of
     * {@link OutboundType#SEND_WITH_ACK} type.
     * </p>
     *
     * @param sndCtx Context of a sent message (same object as in {@link #interceptClientSend(Object, ClientSendContext)}).
     */
    default void interceptClientReceiveVoid(ClientSendContext sndCtx) {
        // No-op.
    }

    /**
     * Intercepts a failure of receiving a response from server.
     *
     * @param err Error
     * @param sndCtx Context of a message (same object as in {@link #interceptClientSend(Object, ClientSendContext)}).
     */
    default void interceptClientReceiveError(Throwable err, ClientSendContext sndCtx) {
        // No-op.
    }

    /**
     * Intercepts an incoming message from a remote client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} when it receives a message from a client. Implementations of this
     * method can decide to transform the message into another message based on some criteria of the {@link ServerReceiveContext}.
     * </p>
     *
     * @param msg Message.
     * @param rcvCtx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #interceptServerSend(Object, ResponseContext, ServerReceiveContext)
     */
    default T interceptServerReceive(T msg, ServerReceiveContext rcvCtx) {
        return null;
    }

    /**
     * Intercepts a response right before sending it back to a client.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} right before sending a response to a client. Implementations of this
     * method can decide to transform the message into another message based on some criteria of the {@link ServerReceiveContext}.
     * </p>
     *
     * @param rsp Response.
     * @param rspCtx Response context.
     * @param rcvCtx Context of a request message (same object as in {@link #interceptServerReceive(Object, ServerReceiveContext)}).
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     *
     * @see #interceptServerReceive(Object, ServerReceiveContext)
     */
    default T interceptServerSend(T rsp, ResponseContext rspCtx, ServerReceiveContext rcvCtx) {
        return null;
    }
}
