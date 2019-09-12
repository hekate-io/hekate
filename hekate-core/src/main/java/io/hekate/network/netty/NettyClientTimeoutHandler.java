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

import io.hekate.network.netty.NetworkProtocol.Heartbeat;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

class NettyClientTimeoutHandler extends SimpleChannelInboundHandler {
    private static class HeartbeatOnlyIdleStateHandler extends IdleStateHandler {
        private boolean notifyRead;

        public HeartbeatOnlyIdleStateHandler(long timeout) {
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

    private static final String CONNECT_TIMEOUT_HANDLER_ID = "connect-timeout-handler";

    private final Logger log;

    private final boolean trace;

    private final String id;

    private final long idleTimeout;

    private final Integer connTimeout;

    private final ChannelFutureListener hbOnFlush;

    private boolean hbFlushed = true;

    private int ignoreTimeouts;

    private boolean handshakeDone;

    public NettyClientTimeoutHandler(
        String id,
        Integer connTimeout,
        long idleTimeout,
        Logger log
    ) {
        this.id = id;
        this.idleTimeout = idleTimeout;
        this.connTimeout = connTimeout;
        this.log = log;
        this.trace = log.isTraceEnabled();

        hbOnFlush = future -> {
            hbFlushed = true;

            if (!future.isSuccess() && future.channel().isOpen()) {
                future.channel().pipeline().fireExceptionCaught(future.cause());
            }
        };
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        if (connTimeout != null && connTimeout > 0) {
            if (trace) {
                log.trace("Registering connect timeout handler [to={}, timeout={}]", id, connTimeout);
            }

            IdleStateHandler connectTimeoutHandler = new IdleStateHandler(connTimeout, 0, 0, TimeUnit.MILLISECONDS);

            ctx.pipeline().addFirst(CONNECT_TIMEOUT_HANDLER_ID, connectTimeoutHandler);
        }

        super.channelRegistered(ctx);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof NettyClientHandshakeEvent) {
            NettyClientHandshakeEvent handshake = (NettyClientHandshakeEvent)evt;

            handshakeDone = true;

            // Unregister connect timeout handler.
            if (ctx.pipeline().get(CONNECT_TIMEOUT_HANDLER_ID) != null) {
                ctx.pipeline().remove(CONNECT_TIMEOUT_HANDLER_ID);
            }

            // Register heartbeat handler.
            mayBeRegisterHeartbeatHandler(handshake, ctx);

            super.userEventTriggered(ctx, evt);
        } else if (evt instanceof AutoReadChangeEvent) {
            if (evt == AutoReadChangeEvent.PAUSE) {
                // Completely ignore read timeouts.
                ignoreTimeouts = -1;
            } else {
                // Ignore next timeout.
                ignoreTimeouts = 1;
            }

            super.userEventTriggered(ctx, evt);
        } else if (evt instanceof IdleStateEvent) {
            IdleStateEvent idle = (IdleStateEvent)evt;

            if (idle.state() == IdleState.WRITER_IDLE) {
                if (hbFlushed) {
                    // Make sure that we don't push multiple heartbeats to the network buffer simultaneously.
                    // Need to perform this check since remote peer can hang and stop reading
                    // while this channel will still be trying to put more and more heartbeats on its send buffer.
                    hbFlushed = false;

                    ctx.writeAndFlush(Heartbeat.INSTANCE).addListener(hbOnFlush);
                }
            } else {
                // Reader idle.
                // Ignore if auto-reading was disabled since in such case we will not read any heartbeats.
                if (ignoreTimeouts != -1 && ctx.channel().config().isAutoRead()) {
                    // Check if timeout should be ignored.
                    if (ignoreTimeouts > 0) {
                        // Decrement the counter of ignored timeouts.
                        ignoreTimeouts--;
                    } else {
                        if (handshakeDone) {
                            ctx.fireExceptionCaught(new SocketTimeoutException("Timeout while reading data from " + id));
                        } else {
                            ctx.fireExceptionCaught(new ConnectTimeoutException("Timeout while connecting to " + id));
                        }
                    }
                }
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Ignore heartbeats.
        if (msg instanceof Heartbeat) {
            if (trace) {
                log.trace("Received heartbeat from server [from={}, message={}]", id, msg);
            }

            return;
        }

        ctx.fireChannelRead(msg);
    }

    private void mayBeRegisterHeartbeatHandler(NettyClientHandshakeEvent evt, ChannelHandlerContext ctx) {
        int interval = evt.hbInterval();
        int threshold = evt.hbLossThreshold();
        boolean disableHeartbeats = evt.isHbDisabled();

        ChannelPipeline pipe = ctx.pipeline();

        if (idleTimeout > 0) {
            if (trace) {
                log.trace("Registering idle connection handler [to={}, idle-timeout={}]", id, idleTimeout);
            }

            pipe.addBefore(ctx.name(), "idle-channel-handler", new HeartbeatOnlyIdleStateHandler(idleTimeout));
        }

        if (interval > 0 && threshold > 0) {
            int readerIdle = interval * threshold;
            int writerIdle = disableHeartbeats ? 0 : interval;

            if (trace) {
                log.trace("Registering heartbeat handler [to={}, reader-idle={}, writer-idle={}]", id, readerIdle, writerIdle);
            }

            pipe.addBefore(ctx.name(), "heartbeat-handler", new IdleStateHandler(readerIdle, writerIdle, 0, TimeUnit.MILLISECONDS));
        }
    }
}
