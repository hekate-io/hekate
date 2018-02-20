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

import io.hekate.network.NetworkClient.State;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.netty.NetworkProtocol.HandshakeAccept;
import io.hekate.network.netty.NetworkProtocol.HandshakeReject;
import io.hekate.network.netty.NetworkProtocol.HandshakeRequest;
import io.hekate.network.netty.NetworkProtocol.Heartbeat;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;

import static io.hekate.network.NetworkClient.State.CONNECTED;
import static io.hekate.network.NetworkClient.State.CONNECTING;
import static io.hekate.network.NetworkClient.State.DISCONNECTED;

class NettyClientProtocolHandler<T> extends SimpleChannelInboundHandler {
    private static final String CONNECT_TIMEOUT_HANDLER_ID = "timeout_handler";

    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private final String id;

    private final int epoch;

    private final String protocol;

    private final int affinity;

    private final T login;

    private final long idleTimeout;

    private final Integer connTimeout;

    private final NettyMetricsSink metrics;

    private final NettyClient<T> client;

    private final NetworkClientCallback<T> callback;

    private final ChannelFutureListener hbOnFlush;

    private boolean hbFlushed = true;

    private int ignoreTimeouts;

    private boolean handshakeDone;

    private State state = CONNECTING;

    public NettyClientProtocolHandler(
        String id,
        int epoch,
        String protocol,
        int affinity,
        T login,
        Integer connTimeout,
        long idleTimeout,
        Logger log,
        NettyMetricsSink metrics,
        NettyClient<T> client,
        NetworkClientCallback<T> callback
    ) {
        this.log = log;
        this.id = id;
        this.epoch = epoch;
        this.protocol = protocol;
        this.affinity = affinity;
        this.login = login;
        this.idleTimeout = idleTimeout;
        this.connTimeout = connTimeout;
        this.metrics = metrics;
        this.client = client;
        this.callback = callback;

        this.debug = log.isDebugEnabled();
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
        super.channelRegistered(ctx);

        if (connTimeout != null && connTimeout > 0) {
            if (debug) {
                log.debug("Registering connect timeout handler [to={}, timeout={}]", id, connTimeout);
            }

            IdleStateHandler connectTimeoutHandler = new IdleStateHandler(connTimeout, 0, 0, TimeUnit.MILLISECONDS);

            ctx.pipeline().addFirst(CONNECT_TIMEOUT_HANDLER_ID, connectTimeoutHandler);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);

        if (client.isSecure()) {
            if (debug) {
                log.debug("Deferred handshake until SSL connection is established [to={}]", id);
            }
        } else {
            handshake(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof SslHandshakeCompletionEvent) {
            if (((SslHandshakeCompletionEvent)evt).isSuccess()) {
                if (debug) {
                    log.debug("SSL connection established [to={}]", id);
                }

                handshake(ctx);
            }

            super.userEventTriggered(ctx, evt);
        } else if (evt instanceof AutoReadChangeEvent) {
            if (evt == AutoReadChangeEvent.PAUSE) {
                // Completely ignore read timeouts.
                ignoreTimeouts = -1;
            } else {
                // Ignore next timeout.
                ignoreTimeouts = 1;
            }
        } else if (evt instanceof IdleStateEvent) {
            if (state == CONNECTING || state == CONNECTED) {
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
                            if (state == CONNECTING) {
                                ctx.fireExceptionCaught(new ConnectTimeoutException("Timeout while connecting to " + id));
                            } else if (state == CONNECTED) {
                                ctx.fireExceptionCaught(new SocketTimeoutException("Timeout while reading data from " + id));
                            }
                        }
                    }
                }
            }
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        if (metrics != null) {
            metrics.onDisconnect();
        }

        if (state == CONNECTING) {
            state = DISCONNECTED;

            ctx.fireExceptionCaught(new ConnectException("Got disconnected on handshake [channel=" + id + ']'));
        } else {
            state = DISCONNECTED;

            super.channelInactive(ctx);
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

        if (handshakeDone) {
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
        } else {
            if (debug) {
                log.debug("Received handshake response [from={}, message={}]", id, msg);
            }

            NetworkProtocol handshakeMsg = (NetworkProtocol)msg;

            if (handshakeMsg.type() == NetworkProtocol.Type.HANDSHAKE_REJECT) {
                HandshakeReject reject = (HandshakeReject)handshakeMsg;

                String reason = reject.reason();

                if (debug) {
                    log.debug("Server rejected connection [to={}, reason={}]", id, reason);
                }

                ctx.fireExceptionCaught(new ConnectException(reason + " [channel=" + id + ']'));
            } else {
                HandshakeAccept accept = (HandshakeAccept)handshakeMsg;

                handshakeDone = true;

                // Unregister connect timeout handler.
                if (ctx.pipeline().get(CONNECT_TIMEOUT_HANDLER_ID) != null) {
                    ctx.pipeline().remove(CONNECT_TIMEOUT_HANDLER_ID);
                }

                int interval = accept.hbInterval();
                int threshold = accept.hbLossThreshold();
                boolean disableHeartbeats = accept.hbDisabled();

                ChannelPipeline pipe = ctx.pipeline();

                // Register heartbeat handler.
                if (interval > 0 && threshold > 0) {
                    int readTimeout = interval * threshold;

                    if (disableHeartbeats) {
                        interval = 0;

                        if (debug) {
                            log.debug("Registering heartbeatless timeout handler [to={}, read-timeout={}]", id, readTimeout);
                        }
                    } else {
                        if (debug) {
                            log.debug("Registering heartbeats handler [to={}, interval={}, loss-threshold={}, read-timeout={}]",
                                id, interval, threshold, readTimeout);
                        }
                    }

                    pipe.addFirst(new IdleStateHandler(readTimeout, interval, 0, TimeUnit.MILLISECONDS));
                }

                // Register idle connection handler.
                if (idleTimeout > 0) {
                    if (debug) {
                        log.debug("Registering idle connection handler [to={}, idle-timeout={}]", id, idleTimeout);
                    }

                    NettyClientIdleStateHandler idleStateHandler = new NettyClientIdleStateHandler(idleTimeout);

                    pipe.addAfter(NettyClient.ENCODER_HANDLER_ID, NettyClientIdleStateHandler.class.getName(), idleStateHandler);
                }

                // Update state and notify on handshake completion.
                state = CONNECTED;

                ctx.fireUserEventTriggered(new HandshakeDoneEvent(epoch));
            }
        }
    }

    private void handshake(ChannelHandlerContext ctx) {
        HandshakeRequest request = new HandshakeRequest(protocol, login, affinity);

        if (debug) {
            log.debug("Connected ...sending handshake request [to={}, from={}, request={}]", id, ctx.channel().localAddress(), request);
        }

        if (metrics != null) {
            metrics.onConnect();
        }

        ctx.writeAndFlush(request);
    }
}
