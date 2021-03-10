/*
 * Copyright 2021 The Hekate Project
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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClient.State;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkSendCallback;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;

import static io.hekate.network.NetworkClient.State.CONNECTED;
import static io.hekate.network.NetworkClient.State.CONNECTING;
import static io.hekate.network.NetworkClient.State.DISCONNECTED;
import static io.hekate.network.NetworkClient.State.DISCONNECTING;

class NettyClientContext<T> {
    private final String id;

    private final AtomicReference<State> state = new AtomicReference<>(DISCONNECTED);

    @ToStringIgnore
    private final InetSocketAddress remoteAddress;

    @ToStringIgnore
    private final Logger log;

    @ToStringIgnore
    private final boolean debug;

    @ToStringIgnore
    private final boolean trace;

    @ToStringIgnore
    private final NetworkFuture<T> connFuture;

    @ToStringIgnore
    private final NetworkFuture<T> discFuture;

    @ToStringIgnore
    private final Channel channel;

    @ToStringIgnore
    private final Codec<Object> codec;

    @ToStringIgnore
    private final NettyWriteQueue writeQueue;

    @ToStringIgnore
    private final NettyMetricsSink metrics;

    @ToStringIgnore
    private final NetworkClient<T> endpoint;

    @ToStringIgnore
    private final NetworkClientCallback<T> callback;

    @ToStringIgnore
    private final EventLoop eventLoop;

    @ToStringIgnore
    private volatile InetSocketAddress localAddress;

    public NettyClientContext(
        InetSocketAddress address,
        Codec<Object> codec,
        NettyMetricsSink metrics,
        NettyClient<T> endpoint,
        EventLoop eventLoop,
        String protocol,
        int affinity,
        T login,
        Integer connectTimeout,
        long idleTimeout,
        SslContext ssl,
        boolean epoll,
        Boolean tcpNoDelay,
        Integer soReceiveBufSize,
        Integer soSendBufSize,
        Boolean soReuseAddress,
        NettySpy spy,
        Logger log,
        NetworkClientCallback<T> callback
    ) {
        this.remoteAddress = address;
        this.codec = codec;
        this.metrics = metrics;
        this.endpoint = endpoint;
        this.eventLoop = eventLoop;
        this.callback = callback;
        this.log = log;
        this.debug = log.isDebugEnabled();
        this.trace = log.isTraceEnabled();

        // Connection identifier.
        this.id = remoteAddress.toString() + ':' + protocol;

        // Connect/disconnect future.
        connFuture = new NetworkFuture<>();
        discFuture = new NetworkFuture<>();

        // Prepare write queue.
        writeQueue = new NettyWriteQueue(false, spy);

        // Prepare Netty bootstrap.
        Bootstrap boot = new Bootstrap();

        if (epoll) {
            boot.channel(EpollSocketChannel.class);
        } else {
            boot.channel(NioSocketChannel.class);
        }

        boot.group(eventLoop);
        boot.remoteAddress(address);

        // Apply configuration options.
        setOpt(boot, ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        setOpt(boot, ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        setOpt(boot, ChannelOption.TCP_NODELAY, tcpNoDelay);
        setOpt(boot, ChannelOption.SO_RCVBUF, soReceiveBufSize);
        setOpt(boot, ChannelOption.SO_SNDBUF, soSendBufSize);
        setOpt(boot, ChannelOption.SO_REUSEADDR, soReuseAddress);

        // Prepare channel handlers.
        boot.handler(new ChannelInitializer() {
            @Override
            protected void initChannel(Channel ch) throws Exception {
                // Build pipeline.
                ChannelPipeline pipeline = ch.pipeline();

                // Configure SSL.
                if (ssl != null) {
                    SslHandler sslHandler = ssl.newHandler(ch.alloc(), AddressUtils.host(address), address.getPort());

                    if (connectTimeout != null && connectTimeout > 0) {
                        sslHandler.setHandshakeTimeoutMillis(connectTimeout);
                    }

                    pipeline.addLast(sslHandler);
                }

                // Configure metrics.
                if (metrics != null) {
                    pipeline.addLast(new ChannelTrafficShapingHandler(0, 0, NettyInternalConst.TRAFFIC_SHAPING_INTERVAL) {
                        @Override
                        protected void doAccounting(TrafficCounter counter) {
                            metrics.onBytesReceived(counter.lastReadBytes());
                            metrics.onBytesSent(counter.lastWrittenBytes());
                        }
                    });
                }

                // Protocol codecs.
                NetworkProtocolCodec protocolCodec = new NetworkProtocolCodec(codec);

                pipeline.addLast(new NetworkProtocolVersion.Encoder());
                pipeline.addLast(protocolCodec.decoder());
                pipeline.addLast(protocolCodec.encoder());

                // Handshake handler.
                pipeline.addLast(new NettyClientHandshakeHandler<>(id, protocol, affinity, login, log, ssl != null));

                // Timeout handler.
                pipeline.addLast(new NettyClientTimeoutHandler(id, connectTimeout, idleTimeout, log));

                // State handler.
                pipeline.addLast(new ChannelInboundHandlerAdapter() {
                    @Override
                    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                        if (evt instanceof NettyClientHandshakeEvent) {
                            if (state.compareAndSet(CONNECTING, CONNECTED)) {
                                if (debug) {
                                    log.debug("Connected [to={}, transport={}, ssl={}]", id, epoll ? "EPOLL" : "NIO", ssl != null);
                                }

                                // Register message handler.
                                ctx.pipeline().addLast(new NettyClientMessageHandler<>(
                                    id,
                                    metrics,
                                    endpoint,
                                    callback,
                                    log
                                ));

                                callback.onConnect(endpoint);

                                connFuture.complete(endpoint);

                                writeQueue.enableWrites(eventLoop);
                            }
                        }

                        super.userEventTriggered(ctx, evt);
                    }

                    @Override
                    public void channelActive(ChannelHandlerContext ctx) throws Exception {
                        localAddress = (InetSocketAddress)ctx.channel().localAddress();

                        if (metrics != null) {
                            metrics.onConnect();
                        }

                        super.channelActive(ctx);
                    }

                    @Override
                    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                        if (metrics != null) {
                            metrics.onDisconnect();
                        }

                        onDisconnect(Optional.empty());

                        super.channelInactive(ctx);
                    }

                    @Override
                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                        if (cause instanceof CodecException) {
                            // Ignore since this is an application-level error.
                            return;
                        }

                        onDisconnect(Optional.of(cause));

                        ctx.close();
                    }
                });
            }
        });

        if (debug) {
            log.debug("Connecting [to={}, transport={}, ssl={}]", id, epoll ? "EPOLL" : "NIO", ssl != null);
        }

        // Initialize channel.
        channel = boot.register().channel();
    }

    public NetworkFuture<T> connect() {
        if (state.compareAndSet(DISCONNECTED, CONNECTING)) {
            channel.connect(remoteAddress).addListener(future -> {
                if (!future.isSuccess()) {
                    if (future.cause() instanceof ClosedChannelException) {
                        onDisconnect(Optional.of(new ConnectException("Got disconnected on handshake [from=" + id + ']')));
                    } else {
                        onDisconnect(Optional.of(future.cause()));
                    }
                }
            });
        }

        return connFuture;
    }

    public NetworkFuture<T> disconnect() {
        if (state.compareAndSet(CONNECTING, DISCONNECTING) || state.compareAndSet(CONNECTED, DISCONNECTING)) {
            channel.close();
        }

        return discFuture;
    }

    public NetworkFuture<T> disconnectFuture() {
        return discFuture;
    }

    public State state() {
        return state.get();
    }

    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    public InetSocketAddress localAddress() {
        return localAddress;
    }

    public Channel channel() {
        return channel;
    }

    public void write(T msg, NetworkSendCallback<T> onSend) {
        if (!validateMessageType(msg, onSend)) {
            return;
        }

        if (debug) {
            log.debug("Sending to server [to={}, message={}]", id, msg);
        }

        // Update metrics.
        if (metrics != null) {
            metrics.onMessageEnqueue();
        }

        // Prepare deferred message.
        DeferredMessage deferredMsg;

        boolean failed = false;

        // Maybe pre-encode message.
        if (codec.isStateful()) {
            deferredMsg = new DeferredMessage(msg, channel);
        } else {
            if (trace) {
                log.trace("Pre-encoding message [to={}, message={}]", id, msg);
            }

            try {
                ByteBuf buf = NetworkProtocolCodec.preEncode(msg, codec, channel.alloc());

                deferredMsg = new DeferredEncodedMessage(buf, msg, channel);
            } catch (CodecException e) {
                deferredMsg = fail(msg, channel, e);

                failed = true;
            }
        }

        // Register listener.
        deferredMsg.addListener((ChannelFuture result) -> {
            if (metrics != null) {
                metrics.onMessageDequeue();
            }

            if (result.isSuccess()) {
                // Successful operation.
                if (debug) {
                    log.debug("Done sending to server [to={}, message={}]", id, msg);
                }

                if (metrics != null) {
                    metrics.onMessageSent();
                }
            } else {
                // Failed operation.
                if (debug) {
                    log.debug("Failed to send to server [to={}, message={}]", id, msg, result.cause());
                }

                // Notify channel pipeline on error (ignore if channel is already closed).
                if (channel.isOpen()) {
                    channel.pipeline().fireExceptionCaught(result.cause());
                }
            }

            // Notify user callback.
            if (onSend != null) {
                Throwable err = NettyErrorUtils.unwrap(result.cause());

                onSend.onComplete(msg, err);
            }
        });

        // Enqueue write operation.
        if (!failed) {
            writeQueue.enqueue(deferredMsg, eventLoop);
        }
    }

    public void pauseReceiver(boolean pause, Consumer<NetworkEndpoint<T>> callback) {
        if (debug) {
            if (pause) {
                log.debug("Pausing outbound receiver [to={}]", id);
            } else {
                log.debug("Resuming outbound receiver [to={}]", id);
            }
        }

        if (eventLoop.inEventLoop()) {
            channel.config().setAutoRead(!pause);

            notifyOnReceivePause(pause, callback, channel);
        } else {
            eventLoop.execute(() -> {
                channel.config().setAutoRead(!pause);

                notifyOnReceivePause(pause, callback, channel);
            });
        }
    }

    private void onDisconnect(Optional<Throwable> rawError) {
        State oldState = state.getAndSet(DISCONNECTED);

        // Proceed only if state changed.
        if (oldState != DISCONNECTED) {
            // Unwrap error.
            Optional<Throwable> err = rawError.map(NettyErrorUtils::unwrap);

            if (debug) {
                if (oldState == CONNECTING && err.isPresent()) {
                    log.debug("Failed to connect [to={}]", id, err.get());
                } else if (oldState == CONNECTED && err.isPresent()) {
                    log.debug("Disconnected on error [from={}, state={}]", id, oldState, err.get());
                } else {
                    log.debug("Disconnected [from={}, state={}]", id, oldState);
                }
            }

            // Ensure that pending messages will fail (since channel is closed).
            writeQueue.dispose(err.orElse(null), eventLoop);

            // Notify callbacks and futures.
            callback.onDisconnect(endpoint, err);

            if (!connFuture.isDone()) {
                if (err.isPresent()) {
                    connFuture.completeExceptionally(err.get());
                } else {
                    connFuture.complete(endpoint);
                }
            }

            discFuture.complete(endpoint);
        }
    }

    private void notifyOnReceivePause(boolean pause, Consumer<NetworkEndpoint<T>> callback, Channel channel) {
        channel.pipeline().fireUserEventTriggered(pause ? AutoReadChangeEvent.PAUSE : AutoReadChangeEvent.RESUME);

        if (callback != null) {
            try {
                callback.accept(endpoint);
            } catch (Throwable e) {
                log.error("Got an unexpected error while notifying callback on network outbound receive status change "
                    + "[pause={}, to={}]", pause, id, e);
            }
        }
    }

    private boolean validateMessageType(T msg, NetworkSendCallback<T> onSend) {
        if (codec.baseType().isInstance(msg)) {
            return true;
        } else {
            String expected = codec.baseType().getName();
            String real = msg.getClass().getName();

            CodecException err = new CodecException("Unsupported message type [expected=" + expected + ", real=" + real + ']');

            if (onSend == null) {
                if (log.isErrorEnabled()) {
                    log.error("Message sending failed.", err);
                }
            } else {
                notifyOnError(msg, onSend, err);
            }

            return false;
        }
    }

    private DeferredMessage fail(T msg, Channel channel, Throwable error) {
        DeferredMessage promise = new DeferredMessage(msg, channel);

        promise.setFailure(error);

        return promise;
    }

    private void notifyOnError(T msg, NetworkSendCallback<T> onSend, Throwable error) {
        try {
            onSend.onComplete(msg, error);
        } catch (Throwable e) {
            log.error("Failed to notify callback on network operation failure [to={}, message={}]", id, msg, e);
        }
    }

    private <V> void setOpt(Bootstrap boot, ChannelOption<V> opt, V value) {
        if (value != null) {
            if (trace) {
                log.trace("Setting option {} = {} [to={}]", opt, value, id);
            }

            boot.option(opt, value);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
