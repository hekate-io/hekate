/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.network.internal.netty;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.Utils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkSendCallback;
import io.hekate.network.internal.netty.NettyWriteQueue.WritePromise;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.network.NetworkClient.State.CONNECTED;
import static io.hekate.network.NetworkClient.State.CONNECTING;
import static io.hekate.network.NetworkClient.State.DISCONNECTED;
import static io.hekate.network.NetworkClient.State.DISCONNECTING;

class NettyClient<T> implements NetworkClient<T> {
    private static class ChannelContext {
        private final Channel channel;

        private final GenericFutureListener<ChannelFuture> writeListener;

        private final Codec<Object> codec;

        public ChannelContext(Channel channel, GenericFutureListener<ChannelFuture> writeListener, Codec<Object> codec) {
            this.channel = channel;
            this.writeListener = writeListener;
            this.codec = codec;
        }

        public Channel getChannel() {
            return channel;
        }

        public Codec<Object> getCodec() {
            return codec;
        }

        public GenericFutureListener<ChannelFuture> getWriteListener() {
            return writeListener;
        }
    }

    static final int TRAFFIC_SHAPING_INTERVAL = 1000;

    static final String ENCODER_HANDLER_ID = "encoder";

    private static final String DECODER_HANDLER_ID = "decoder";

    private final ReentrantLock lock = new ReentrantLock();

    private final CodecFactory<Object> codecFactory;

    private final String protocol;

    private final Integer connectTimeout;

    private final long idleTimeout;

    private final boolean tcpNoDelay;

    private final Integer soReceiveBufferSize;

    private final Integer soSendBufferSize;

    private final Boolean soReuseAddress;

    private final NettyMetricsCallback metrics;

    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private final boolean useEpoll;

    private final EventLoop eventLoop;

    private final NettyWriteQueue writeQueue = new NettyWriteQueue();

    private NetworkFuture<T> connectFuture;

    private NetworkFuture<T> disconnectFuture;

    private int epoch;

    private State state = DISCONNECTED;

    private volatile ChannelContext channelCtx;

    private volatile Object userContext;

    private volatile InetSocketAddress address;

    private volatile InetSocketAddress localAddress;

    @SuppressWarnings("unchecked")
    public NettyClient(NettyClientFactory<T> factory) {
        assert factory != null : "Configuration is null.";

        ConfigCheck check = ConfigCheck.get(NettyClientFactory.class);

        check.notEmpty(factory.getProtocol(), "protocol");
        check.notNull(factory.getCodecFactory(), "codec factory");
        check.notNull(factory.getEventLoopGroup(), "event loops group");

        if (factory.getLoggerCategory() == null) {
            log = LoggerFactory.getLogger(NettyClient.class);
        } else {
            log = LoggerFactory.getLogger(factory.getLoggerCategory());
        }

        debug = log.isDebugEnabled();
        trace = log.isTraceEnabled();

        connectTimeout = factory.getConnectTimeout();
        idleTimeout = factory.getIdleTimeout();
        tcpNoDelay = factory.getTcpNoDelay();
        soReceiveBufferSize = factory.getSoReceiveBufferSize();
        soSendBufferSize = factory.getSoSendBufferSize();
        soReuseAddress = factory.getSoReuseAddress();
        codecFactory = (CodecFactory<Object>)factory.getCodecFactory();
        protocol = factory.getProtocol();
        useEpoll = factory.getEventLoopGroup() instanceof EpollEventLoopGroup;
        eventLoop = factory.getEventLoopGroup().next();

        metrics = factory.getMetrics();
    }

    @Override
    public String getProtocol() {
        return protocol;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return address;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C> C getContext() {
        return (C)userContext;
    }

    @Override
    public void setContext(Object ctx) {
        this.userContext = ctx;
    }

    @Override
    public NetworkFuture<T> connect(InetSocketAddress address, NetworkClientCallback<T> callback) {
        return connect(address, null, callback);
    }

    @Override
    public NetworkFuture<T> connect(InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        return doConnect(true, address, login, callback);
    }

    @Override
    public NetworkFuture<T> ensureConnected(InetSocketAddress address, NetworkClientCallback<T> callback) {
        return ensureConnected(address, null, callback);
    }

    @Override
    public NetworkFuture<T> ensureConnected(InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        return doConnect(false, address, login, callback);
    }

    @Override
    public State getState() {
        lock.lock();

        try {
            return state;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
        disconnect().join();
    }

    @Override
    public void send(T msg) {
        doSend(msg, null);
    }

    @Override
    public void send(T msg, NetworkSendCallback<T> callback) {
        doSend(msg, callback);
    }

    @Override
    public void pauseReceiving(Consumer<NetworkEndpoint<T>> callback) {
        pauseReceiver(true, callback);
    }

    @Override
    public void resumeReceiving(Consumer<NetworkEndpoint<T>> callback) {
        pauseReceiver(false, callback);
    }

    @Override
    public boolean isReceiving() {
        ChannelContext localCtx = this.channelCtx;

        return localCtx != null && localCtx.getChannel().config().isAutoRead();
    }

    @Override
    public NetworkFuture<T> disconnect() {
        return withLock(() -> {
            if (state == DISCONNECTING) {
                // Already disconnecting.
                return disconnectFuture;
            } else if (state == CONNECTING || state == CONNECTED) {
                // Update state.
                if (debug) {
                    log.debug("Updated connection state [from={}, to={}, address={}]", state, DISCONNECTING, address);
                }

                state = DISCONNECTING;

                channelCtx.getChannel().close();

                channelCtx = null;

                return disconnectFuture;
            } else {
                // Not connected.
                if (trace) {
                    log.trace("Skipped disconnect request since client is already in {} state.", state);
                }

                if (disconnectFuture == null) {
                    return newCompletedFuture();
                } else {
                    return disconnectFuture;
                }
            }
        });
    }

    private void pauseReceiver(boolean pause, Consumer<NetworkEndpoint<T>> callback) {
        ChannelContext localCtx = this.channelCtx;

        if (localCtx != null) {
            if (debug) {
                if (pause) {
                    log.debug("Pausing outbound receiver [address={}, protocol={}]", address, protocol);
                } else {
                    log.debug("Resuming outbound receiver [address={}, protocol={}]", address, protocol);
                }
            }

            Channel channel = localCtx.getChannel();
            EventLoop eventLoop = channel.eventLoop();

            if (eventLoop.inEventLoop()) {
                channel.config().setAutoRead(!pause);

                notifyOnReceivePause(pause, callback, channel);
            } else {
                eventLoop.execute(() -> {
                        channel.config().setAutoRead(!pause);

                        notifyOnReceivePause(pause, callback, channel);
                    }
                );
            }
        } else if (callback != null) {
            callback.accept(this);
        }
    }

    private void notifyOnReceivePause(boolean pause, Consumer<NetworkEndpoint<T>> callback, Channel channel) {
        assert channel.eventLoop().inEventLoop() : "Must be on event loop thread.";

        channel.pipeline().fireUserEventTriggered(pause ? AutoReadChangeEvent.PAUSE : AutoReadChangeEvent.RESUME);

        if (callback != null) {
            try {
                callback.accept(this);
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error while notifying callback on network outbound receive status change "
                    + "[pause={}, address={}, protocol={}]", pause, address, protocol, e);
            }
        }
    }

    private NetworkFuture<T> doConnect(boolean required, InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        ArgAssert.notNull(address, "Address");
        ArgAssert.notNull(callback, "Callback");

        ChannelFuture nettyChannelFuture;
        NetworkFuture<T> localConnectFuture;
        NetworkFuture<T> localDisconnectFuture;

        lock.lock();

        try {
            if (state == CONNECTING || state == CONNECTED) {
                if (required) {
                    throw new IllegalStateException("Client is in " + state + " state [address=" + address + ']');
                } else {
                    return connectFuture;
                }
            }

            if (eventLoop.isTerminated()) {
                throw new IllegalStateException("I/O thread pools terminated.");
            }

            if (debug) {
                log.debug("Connecting [address={}, protocol={}]", address, protocol);
            }

            this.address = address;

            // Prepare Netty bootstrap.
            Bootstrap bootstrap = new Bootstrap();

            if (useEpoll) {
                if (debug) {
                    log.debug("Using EPOLL socket channel [address={}, protocol={}]", address, protocol);
                }

                bootstrap.channel(EpollSocketChannel.class);
            } else {
                if (debug) {
                    log.debug("Using NIO socket channel [address={}, protocol={}]", address, protocol);
                }

                bootstrap.channel(NioSocketChannel.class);
            }

            bootstrap.group(eventLoop);
            bootstrap.remoteAddress(address);
            bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            // Apply configuration options.
            setOpts(bootstrap);

            // Increment connection epoch (set to 1 if we reached max int value).
            int localEpoch = this.epoch = epoch < Integer.MAX_VALUE ? epoch + 1 : 1;

            // Prepare connect/disconnect future.
            connectFuture = localConnectFuture = new NetworkFuture<>();
            disconnectFuture = localDisconnectFuture = new NetworkFuture<>();

            // Update state.
            state = CONNECTING;

            // Prepare listener that should be attached to all message write futures.
            GenericFutureListener<ChannelFuture> allWritesListener = (ChannelFuture future) -> {
                if (future.isSuccess()) {
                    // Notify metrics on successful operation.
                    if (metrics != null) {
                        metrics.onMessageDequeue();

                        metrics.onMessageSent();
                    }
                } else {
                    // Notify metrics on failed operation.
                    if (metrics != null) {
                        metrics.onMessageDequeue();

                        metrics.onMessageSendError();
                    }

                    ChannelPipeline pipeline = future.channel().pipeline();

                    // Notify on error (only if pipeline is not empty).
                    if (pipeline.last() != null) {
                        future.channel().pipeline().fireExceptionCaught(future.cause());
                    }
                }
            };

            // Prepare codec.
            Codec<Object> codec = codecFactory.createCodec();

            // Prepare channel handlers.
            bootstrap.handler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    if (debug) {
                        log.debug("Initializing channel pipeline...");
                    }

                    NettyClient<T> client = NettyClient.this;

                    NettyClientHandler<T> msgHandler = new NettyClientHandler<>(localEpoch, protocol, login, connectTimeout,
                        idleTimeout, address, log, metrics, client, callback);

                    NettyClientDeferHandler<T> deferHandler = new NettyClientDeferHandler<>(log);

                    pipeline.addFirst(deferHandler);
                    pipeline.addFirst(msgHandler);

                    NetworkProtocolCodec netCodec = new NetworkProtocolCodec(codec);

                    pipeline.addFirst(ENCODER_HANDLER_ID, netCodec.getEncoder());
                    pipeline.addFirst(DECODER_HANDLER_ID, netCodec.getDecoder());

                    if (metrics != null) {
                        if (debug) {
                            log.debug("Registering metrics handler.");
                        }

                        pipeline.addFirst(new ChannelTrafficShapingHandler(0, 0, TRAFFIC_SHAPING_INTERVAL) {
                            @Override
                            protected void doAccounting(TrafficCounter counter) {
                                metrics.onBytesReceived(counter.lastReadBytes());
                                metrics.onBytesSent(counter.lastWrittenBytes());
                            }
                        });
                    }

                    pipeline.addLast(new ChannelInboundHandlerAdapter() {
                        private Throwable firstError;

                        @Override
                        public void channelActive(ChannelHandlerContext ctx) throws Exception {
                            localAddress = (InetSocketAddress)ctx.channel().localAddress();

                            super.channelActive(ctx);
                        }

                        @Override
                        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                            if (evt instanceof HandshakeDoneEvent) {
                                HandshakeDoneEvent handshake = (HandshakeDoneEvent)evt;

                                // Try to update state and decide on whether the user callback should be notified.
                                boolean notify = withLock(() -> {
                                    if (handshake.getEpoch() == client.epoch && state == CONNECTING) {
                                        if (debug) {
                                            log.debug("Updated connection state [from={}, to={}, address={}]", state, CONNECTED, address);
                                        }

                                        state = CONNECTED;

                                        return true;
                                    } else {
                                        return false;
                                    }
                                });

                                // Notify callback.
                                if (notify) {
                                    callback.onConnect(client);
                                }

                                // Notify connect future.
                                localConnectFuture.complete(client);
                            }

                            super.userEventTriggered(ctx, evt);
                        }

                        @Override
                        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) throws Exception {
                            if (firstError != null || error instanceof CodecException) {
                                // Ignore since this is an application-level error.
                                return;
                            }

                            // Check if we are connected or connecting.
                            boolean connected = withLock(() ->
                                state == CONNECTING || state == CONNECTED
                            );

                            if (connected) {
                                // Store the first error for user callback notification.
                                firstError = error;
                            }

                            if (error instanceof IOException) {
                                if (connected && debug) {
                                    log.debug("Closing outbound network connection due to I/O error "
                                        + "[protocol={}, address={}, state={}, cause={}]", protocol, address, state, error.toString());
                                }
                            } else {
                                log.error("Outbound network connection failure "
                                    + "[protocol={}, address={}, state={}]", protocol, address, state, error);
                            }

                            ctx.close();
                        }

                        @Override
                        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
                            super.channelUnregistered(ctx);

                            boolean wasConnecting = withLock(() -> {
                                if (localEpoch == client.epoch) {
                                    if (debug) {
                                        log.debug("Updated connection state [from={}, to={}, address={}]", state, DISCONNECTED, address);
                                    }

                                    try {
                                        return state == CONNECTING;
                                    } finally {
                                        // Make sure that state is updated and cleanup actions are performed.
                                        state = DISCONNECTED;

                                        cleanup();
                                    }

                                }

                                return false;
                            });

                            callback.onDisconnect(client, Optional.ofNullable(firstError));

                            // Notify futures only if we were NOT in CONNECTING state or if we are aware of an error,
                            // otherwise we can mis an error and notify future on false success.
                            // If not notified here then notification will be performed by netty channel future listener.
                            if (firstError != null || !wasConnecting) {
                                if (!localConnectFuture.isDone()) {
                                    if (firstError == null) {
                                        localConnectFuture.complete(client);
                                    } else {
                                        localConnectFuture.completeExceptionally(firstError);
                                    }
                                }

                                localDisconnectFuture.complete(client);
                            }
                        }
                    });

                    if (debug) {
                        log.debug("Done initializing channel pipeline.");
                    }
                }
            });

            // Connect channel.
            nettyChannelFuture = bootstrap.connect();

            Channel channel = nettyChannelFuture.channel();

            channelCtx = new ChannelContext(channel, allWritesListener, codec);
        } finally {
            lock.unlock();
        }

        // Add listener that will perform cleanup in case of connect failure.
        nettyChannelFuture.addListener((ChannelFutureListener)future -> {
            Throwable cause = future.cause();

            if (cause != null) {
                if (future.channel().pipeline().last() == null) {
                    // Pipeline not initialized yet -> dispose connection in place.
                    try {
                        callback.onDisconnect(this, Optional.of(cause));
                    } finally {
                        // Notify futures at all cost.
                        try {
                            localConnectFuture.completeExceptionally(cause);
                        } finally {
                            try {
                                localDisconnectFuture.complete(NettyClient.this);
                            } finally {
                                disconnect();
                            }
                        }
                    }
                } else {
                    // Pipeline already initialized -> fire error to the pipeline.
                    future.channel().pipeline().fireExceptionCaught(cause);
                }
            }
        });

        return localConnectFuture;
    }

    private void cleanup() {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        channelCtx = null;
        address = null;
        localAddress = null;
        connectFuture = null;
    }

    private void doSend(T msg, NetworkSendCallback<T> onSend) {
        ChannelContext localCtx = this.channelCtx;

        if (localCtx == null) {
            // Notify on channel close error.
            if (metrics != null) {
                metrics.onMessageSendError();
            }

            if (onSend != null) {
                boolean notified = false;

                // Try to notify via channel's event loop.
                if (!eventLoop.isShutdown()) {
                    try {
                        eventLoop.execute(() ->
                            notifyOnChannelClose(msg, onSend)
                        );

                        notified = true;
                    } catch (RejectedExecutionException e) {
                        if (debug) {
                            log.debug("Failed to notify send callback on channel close error. Will retry via fallback pool.");
                        }
                    }
                }

                // If couldn't notify via channel's event loop then use common fork-join pool.
                if (!notified) {
                    Utils.fallbackExecutor().execute(() ->
                        notifyOnChannelClose(msg, onSend)
                    );
                }
            }
        } else {
            // Write message to the channel.
            write(msg, onSend, localCtx);
        }
    }

    private void write(T msg, NetworkSendCallback<T> onSend, ChannelContext ctx) {
        if (debug) {
            log.debug("Sending message [address={}, message={}]", address, msg);
        }

        Channel channel = ctx.getChannel();
        Codec<Object> codec = ctx.getCodec();

        // Update metrics.
        if (metrics != null) {
            metrics.onMessageEnqueue();
        }

        boolean failed = false;

        // Prepare write promise.
        WritePromise promise;

        // Maybe pre-encode message.
        if (codec.isStateful()) {
            promise = new WritePromise(msg, channel);
        } else {
            if (debug) {
                log.debug("Pre-encoding message [address={}, message={}]", address, msg);
            }

            try {
                ByteBuf buf = NetworkProtocolCodec.preEncode(msg, codec, channel.alloc());

                promise = new WritePromise(buf, channel);
            } catch (CodecException e) {
                promise = fail(msg, channel, e);

                failed = true;
            }
        }

        // Register listener.
        promise.addListener((ChannelFuture result) -> {
            if (debug) {
                if (result.isSuccess()) {
                    log.debug("Done sending [address={}, message={}]", address, msg);
                } else {
                    log.debug("Failed to send a message [address={}, message={}]", address, msg, result.cause());
                }
            }

            // Notify common listener.
            ctx.getWriteListener().operationComplete(result);

            // Notify user callback.
            if (onSend != null) {
                onSend.onComplete(msg, Optional.ofNullable(result.cause()), this);
            }
        });

        // Enqueue write operation.
        if (!failed) {
            writeQueue.enqueue(promise, eventLoop);
        }
    }

    private WritePromise fail(T msg, Channel channel, Throwable error) {
        WritePromise promise = new WritePromise(msg, channel);

        promise.setFailure(error);

        return promise;
    }

    private void notifyOnChannelClose(T msg, NetworkSendCallback<T> onSend) {
        try {
            onSend.onComplete(msg, Optional.of(new ClosedChannelException()), this);
        } catch (RuntimeException | Error e) {
            log.error("Failed to notify callback on network operation failure "
                + "[protocol={}, address={}, message={}]", protocol, address, msg, e);
        }
    }

    private void setOpts(Bootstrap bootstrap) {
        setOpt(bootstrap, ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        setOpt(bootstrap, ChannelOption.TCP_NODELAY, tcpNoDelay);
        setOpt(bootstrap, ChannelOption.SO_RCVBUF, soReceiveBufferSize);
        setOpt(bootstrap, ChannelOption.SO_SNDBUF, soSendBufferSize);
        setOpt(bootstrap, ChannelOption.SO_REUSEADDR, soReuseAddress);
    }

    private <O> void setOpt(Bootstrap bootstrap, ChannelOption<O> opt, O value) {
        if (value != null) {
            if (trace) {
                log.trace("Setting option {} = {} [address={}, protocol={}]", opt, value, address, protocol);
            }

            bootstrap.option(opt, value);
        }
    }

    private NetworkFuture<T> newCompletedFuture() {
        NetworkFuture<T> future = new NetworkFuture<>();

        future.complete(this);

        return future;
    }

    private <V> V withLock(Supplier<V> task) {
        lock.lock();

        try {
            return task.get();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        State localState;
        InetSocketAddress localAddr;

        lock.lock();

        try {
            localState = state;
            localAddr = address;
        } finally {
            lock.unlock();
        }

        return getClass().getSimpleName() + "["
            + "address=" + localAddr
            + ", protocol=" + protocol
            + ", state=" + localState
            + ']';
    }
}
