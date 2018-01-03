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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.messaging.MessageInterceptor;
import io.hekate.util.format.ToString;

class MessageRoute<T> implements MessageInterceptor.OutboundContext {
    private final MessagingClient<T> client;

    private final ClusterTopology topology;

    private final MessageContext<T> ctx;

    private final MessageInterceptor<T> interceptor;

    public MessageRoute(MessagingClient<T> client, ClusterTopology topology, MessageContext<T> ctx, MessageInterceptor<T> interceptor) {
        this.client = client;
        this.topology = topology;
        this.ctx = ctx;
        this.interceptor = interceptor;
    }

    public MessagingClient<T> client() {
        return client;
    }

    @Override
    public ClusterNode receiver() {
        return client.node();
    }

    @Override
    public ClusterTopology topology() {
        return topology;
    }

    public T preparePayload() {
        if (interceptor == null) {
            return ctx.originalMessage();
        } else {
            return interceptor.interceptOutbound(ctx.originalMessage(), this);
        }
    }

    public MessageContext<T> ctx() {
        return ctx;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
