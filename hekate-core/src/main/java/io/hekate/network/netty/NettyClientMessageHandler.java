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

package io.hekate.network.netty;

import io.hekate.network.NetworkClientCallback;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;

class NettyClientMessageHandler<T> extends SimpleChannelInboundHandler {
    private final Logger log;

    private final boolean trace;

    private final String id;

    private final NettyMetricsSink metrics;

    private final NettyClient<T> client;

    private final NetworkClientCallback<T> callback;

    public NettyClientMessageHandler(
        String id,
        NettyMetricsSink metrics,
        NettyClient<T> client,
        NetworkClientCallback<T> callback,
        Logger log
    ) {
        this.id = id;
        this.metrics = metrics;
        this.client = client;
        this.callback = callback;
        this.log = log;
        this.trace = log.isTraceEnabled();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        NettyMessage netMsg = (NettyMessage)msg;

        try {
            netMsg.prepare(log);

            if (trace) {
                log.trace("Message buffer prepared [from={}, buffer={}]", id, netMsg);
            }

            if (metrics != null) {
                metrics.onMessageReceived();
            }

            callback.onMessage(netMsg.cast(), client);
        } finally {
            netMsg.release();
        }
    }
}
