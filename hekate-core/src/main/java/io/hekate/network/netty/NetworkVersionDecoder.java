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
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.net.SocketAddress;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NetworkVersionDecoder extends ByteToMessageDecoder {
    private enum State {
        READ_MAGIC,

        READ_VERSION
    }

    private static final Logger log = LoggerFactory.getLogger(NetworkVersionDecoder.class);

    private State state = State.READ_MAGIC;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        switch (state) {
            case READ_MAGIC: {
                if (canReadInt(in)) {
                    if (in.readInt() == Utils.MAGIC_BYTES) {
                        state = State.READ_VERSION;
                    } else {
                        if (log.isWarnEnabled()) {
                            SocketAddress address = ctx.channel().remoteAddress();

                            log.warn("Rejected connection from an unknown software [address={}]", address);
                        }

                        reject(ctx, in);
                    }
                }

                break;
            }
            case READ_VERSION: {
                if (canReadInt(in)) {
                    int ver = in.readInt();

                    if (ver == NetworkProtocol.VERSION) {
                        // Accept version and unregister this handler since all of its checks must be performed only once.
                        ctx.pipeline().remove(this);
                    } else {
                        if (log.isWarnEnabled()) {
                            log.warn("Rejected connection due to an unsupported network protocol version "
                                + "[local-ver={}, remote-ver={}]", ver, NetworkProtocol.VERSION);
                        }

                        reject(ctx, in);
                    }

                }

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected state: " + state);
            }
        }
    }

    private void reject(ChannelHandlerContext ctx, ByteBuf data) {
        // Drop remaining data that can't be processed.
        data.skipBytes(data.readableBytes());

        ctx.close();
    }

    private boolean canReadInt(ByteBuf in) {
        return in.readableBytes() >= Integer.BYTES;
    }
}
