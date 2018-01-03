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

import io.hekate.core.internal.util.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

@Sharable
final class NetworkVersionEncoder extends MessageToMessageEncoder<ByteBuf> {
    public static final NetworkVersionEncoder INSTANCE = new NetworkVersionEncoder();

    private NetworkVersionEncoder() {
        // No-op.
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        int bufSize = Integer.BYTES * 2;

        out.add(ctx.alloc().ioBuffer(bufSize, bufSize)
            .writeInt(Utils.MAGIC_BYTES)
            .writeInt(NetworkProtocol.VERSION)
        );

        out.add(msg.retain());

        ctx.pipeline().remove(this);
    }
}
