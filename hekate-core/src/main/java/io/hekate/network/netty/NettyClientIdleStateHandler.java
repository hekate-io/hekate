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

package io.hekate.network.netty;

import io.hekate.network.netty.NetworkProtocol.Heartbeat;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.timeout.IdleStateHandler;
import java.util.concurrent.TimeUnit;

class NettyClientIdleStateHandler extends IdleStateHandler {
    private boolean notifyRead;

    public NettyClientIdleStateHandler(long timeout) {
        super(0, 0, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof Heartbeat) {
            ctx.fireChannelRead(msg);
        } else {
            notifyRead = true;

            super.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        if (notifyRead) {
            notifyRead = false;

            super.channelReadComplete(ctx);
        } else {
            ctx.fireChannelReadComplete();
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof Heartbeat) {
            ctx.write(msg, promise);
        } else {
            super.write(ctx, msg, promise);
        }
    }
}
