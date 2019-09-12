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

import io.hekate.network.netty.NetworkProtocol.HandshakeAccept;
import io.hekate.network.netty.NetworkProtocol.HandshakeReject;
import io.hekate.network.netty.NetworkProtocol.HandshakeRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import java.net.ConnectException;
import org.slf4j.Logger;

class NettyClientHandshakeHandler<T> extends SimpleChannelInboundHandler {
    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private final String id;

    private final String protocol;

    private final int affinity;

    private final T login;

    private final boolean ssl;

    public NettyClientHandshakeHandler(
        String id,
        String protocol,
        int affinity,
        T login,
        Logger log,
        boolean ssl
    ) {
        this.id = id;
        this.protocol = protocol;
        this.affinity = affinity;
        this.login = login;
        this.ssl = ssl;
        this.log = log;
        this.debug = log.isDebugEnabled();
        this.trace = log.isTraceEnabled();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        if (ssl) {
            if (trace) {
                log.trace("Deferred handshake until SSL connection is established [to={}]", id);
            }
        } else {
            handshake(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent) {
            if (((SslHandshakeCompletionEvent)evt).isSuccess()) {
                if (trace) {
                    log.trace("SSL connection established [to={}]", id);
                }

                handshake(ctx);
            }
        }

        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        ctx.fireExceptionCaught(new ConnectException("Got disconnected on handshake [channel=" + id + ']'));
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (trace) {
            log.trace("Received handshake response [from={}, message={}]", id, msg);
        }

        NetworkProtocol handshakeMsg = (NetworkProtocol)msg;

        if (handshakeMsg.type() == NetworkProtocol.Type.HANDSHAKE_REJECT) {
            HandshakeReject reject = (HandshakeReject)handshakeMsg;

            String reason = reject.reason();

            if (debug) {
                log.debug("Server rejected connection [to={}, reason={}]", id, reason);
            }

            ctx.fireExceptionCaught(new ConnectException(reason));
        } else {
            HandshakeAccept accept = (HandshakeAccept)handshakeMsg;

            // Unregister self from the pipeline (handshake is a one time event).
            ctx.pipeline().remove(this);

            // Fire handshake event.
            ctx.fireUserEventTriggered(new NettyClientHandshakeEvent(accept));
        }
    }

    private void handshake(ChannelHandlerContext ctx) {
        HandshakeRequest request = new HandshakeRequest(protocol, login, affinity);

        if (trace) {
            log.trace("Connected ...sending handshake request [to={}, from={}, request={}]", id, ctx.channel().localAddress(), request);
        }

        ctx.writeAndFlush(request);
    }
}
