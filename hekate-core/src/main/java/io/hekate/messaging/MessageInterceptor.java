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

package io.hekate.messaging;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.loadbalance.LoadBalancer;

/**
 * Message interceptor.
 *
 * <p>
 * This interface can be {@link MessagingChannelConfig#setInterceptor(MessageInterceptor) registered} to the messaging channel in order to
 * intercept and transform messages.
 * </p>
 *
 * @param <T> Base type fo messages that can be handled by this interceptor.
 *
 * @see MessagingChannelConfig#setInterceptor(MessageInterceptor)
 */
public interface MessageInterceptor<T> {
    /**
     * Context for {@link #interceptOutbound(Object, OutboundContext)}.
     */
    interface OutboundContext {
        /**
         * Target node that was selected by the {@link LoadBalancer}.
         *
         * @return Target node that was selected by the {@link LoadBalancer}.
         */
        ClusterNode receiver();

        /**
         * Cluster topology that was used by the {@link LoadBalancer}.
         *
         * @return Cluster topology that was used by the {@link LoadBalancer}.
         */
        ClusterTopology topology();
    }

    /**
     * Context for {@link #interceptInbound(Object, InboundContext)}.
     */
    interface InboundContext {
        /**
         * Local node.
         *
         * @return Local node.
         */
        ClusterNode localNode();
    }

    /**
     * Context for {@link #interceptReply(Object, ReplyContext)} .
     */
    interface ReplyContext {
        /**
         * Local node.
         *
         * @return Local node.
         */
        ClusterNode localNode();
    }

    /**
     * Intercepts the outbound message.
     *
     * <p>
     * This method gets called by the {@link MessagingChannel} after the target node had been selected by the {@link LoadBalancer} for the
     * specified outbound message. Implementations of this method can decide to transform the message into another message based on some
     * criteria of the {@link OutboundContext}.
     * </p>
     *
     * @param msg Message.
     * @param ctx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be send to the target node.
     */
    default T interceptOutbound(T msg, OutboundContext ctx) {
        return msg;
    }

    /**
     * Intercepts the inbound message.
     *
     * @param msg Message.
     * @param ctx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original message should be processed by the {@link MessageReceiver}.
     */
    default T interceptInbound(T msg, InboundContext ctx) {
        return msg;
    }

    /**
     * Intercepts the outbound reply.
     *
     * @param msg Message.
     * @param ctx Context.
     *
     * @return Transformed message or the original message if transformation is not required. Returning {@code null} will be interpreted as
     * if no transformation had been applied and the original reply message should be sent.
     *
     * @see Message#reply(Object)
     * @see Message#partialReply(Object)
     */
    default T interceptReply(T msg, ReplyContext ctx) {
        return msg;
    }
}
