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

    private class NettyClientStateHandler<V> extends ChannelInboundHandlerAdapter implements ChannelFutureListener {
        private final String id;

        private final NettyClient<V> client;

        private final int localEpoch;

        private final NetworkClientCallback<V> callback;

        private final NetworkFuture<V> localConnectFuture;

        private final NetworkFuture<V> localDisconnectFuture;

        private Throwable firstError;

        private boolean disconnected;

        private boolean connectComplete;

        public NettyClientStateHandler(NettyClient<V> client, NetworkClientCallback<V> callback) {
            this.client = client;
            this.id = client.id();
            this.localEpoch = client.epoch;
            this.callback = callback;
            this.localConnectFuture = client.connectFuture;
            this.localDisconnectFuture = client.disconnectFuture;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            connectComplete = true;

            if (future.isSuccess()) {
                if (trace) {
                    log.trace("Channel connect future completed successfully [channel={}]", id);
                }
            } else {
                if (trace) {
                    log.trace("Notifying on channel connect future failure [channel={}]", id, future.cause());
                }

                firstError = future.cause();

                if (future.channel().pipeline().get(NettyClientDeferHandler.class.getName()) != null) {
                    future.channel().pipeline().fireExceptionCaught(future.cause());
                }

                becomeDisconnected();
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            localAddress = (InetSocketAddress)ctx.channel().localAddress();

            if (trace) {
                log.debug("Channel is active [channel={}]", id);
            }

            super.channelActive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof HandshakeDoneEvent) {
                HandshakeDoneEvent handshake = (HandshakeDoneEvent)evt;

                if (trace) {
                    log.trace("Processing handshake event [channel={}, event={}]", id, evt);
                }

                // Try to update state and decide on whether the user callback should be notified.
                boolean notify = withLock(() -> {
                    if (handshake.getEpoch() == client.epoch && state == CONNECTING) {
                        if (debug) {
                            log.debug("Updated connection state [from={}, to={}, channel={}]", state, CONNECTED, id);
                        }

                        state = CONNECTED;

                        return true;
                    } else {
                        return false;
                    }
                });

                // Notify callback and future.
                if (notify) {
                    callback.onConnect(client);

                    localConnectFuture.complete(client);
                } else {
                    if (trace) {
                        log.trace("Skipped processing of handshake event [channel={}, event={}]", id, evt);
                    }
                }
            }

            super.userEventTriggered(ctx, evt);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) throws Exception {
            if (firstError != null) {
                // Handle only the very first error.
                return;
            }

            if (error instanceof CodecException) {
                // Ignore since this is an application-level error.
                return;
            }

            if (trace) {
                log.trace("Exception caught in state handler [channel={}]", id, error);
            }

            firstError = error;

            ctx.close();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            if (trace) {
                log.trace("Channel unregistered [channel={}]", id);
            }

            super.channelUnregistered(ctx);

            becomeDisconnected();
        }

        private void becomeDisconnected() {
            if (!connectComplete) {
                if (trace) {
                    log.trace("Skipped on-disconnect notification since connect is not complete yet [channel={}, state={}]", id, state);
                }
            } else if (disconnected) {
                if (trace) {
                    log.trace("Skipped on-disconnect notification since already disconnected [channel={}, state={}]", id, state);
                }
            } else {
                disconnected = true;

                if (firstError != null) {
                    if (firstError instanceof IOException) {
                        if (debug) {
                            log.debug("Closing outbound connection due to I/O error [channel={}, state={}, cause={}]", id, state,
                                firstError.toString());
                        }
                    } else {
                        log.error("Outbound connection failure [channel={}, state={}]", id, state, firstError);
                    }
                }

                boolean ignoreError = withLock(() -> {
                    if (localEpoch == client.epoch && state != DISCONNECTED) {
                        if (debug) {
                            log.debug("Updated connection state [from={}, to={}, channel={}]", state, DISCONNECTED, id);
                        }

                        try {
                            return state == DISCONNECTING;
                        } finally {
                            // Make sure that state is updated and cleanup actions are performed.
                            state = DISCONNECTED;

                            cleanup();
                        }
                    }

                    return true;
                });

                Optional<Throwable> finalError = ignoreError ? Optional.empty() : Optional.ofNullable(firstError);

                if (trace) {
                    log.trace("Notifying callbacks on disconnect [channel={}, error={}]", id, finalError);
                }

                callback.onDisconnect(client, finalError);

                if (finalError.isPresent()) {
                    localConnectFuture.completeExceptionally(finalError.get());
                } else {
                    localConnectFuture.complete(client);
                }

                localDisconnectFuture.complete(client);
            }
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
                    log.debug("Updated connection state [from={}, to={}, channel={}]", state, DISCONNECTING, id());
                }

                state = DISCONNECTING;

                if (trace) {
                    log.trace("Invoking close [channel={}", id());
                }

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
                    log.debug("Pausing outbound receiver [channel={}]", id());
                } else {
                    log.debug("Resuming outbound receiver [channel={}]", id());
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
                    + "[pause={}, channel={}]", pause, id(), e);
            }
        }
    }

    private NetworkFuture<T> doConnect(boolean required, InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        ArgAssert.notNull(address, "Address");
        ArgAssert.notNull(callback, "Callback");

        NetworkFuture<T> localConnectFuture;
        Runnable afterLock;

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
                throw new IllegalStateException("I/O thread pool terminated.");
            }

            this.address = address;

            // Prepare Netty bootstrap.
            Bootstrap bootstrap = new Bootstrap();

            if (useEpoll) {
                if (debug) {
                    log.debug("Connecting [channel={}, transport=EPOLL]", id());
                }

                bootstrap.channel(EpollSocketChannel.class);
            } else {
                if (debug) {
                    log.debug("Connecting [channel={}, transport=NIO]", id());
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
            disconnectFuture = new NetworkFuture<>();

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

                    Channel channel = future.channel();

                    // Notify channel pipeline on error (ignore if channel is already closed).
                    if (channel.isOpen()) {
                        channel.pipeline().fireExceptionCaught(future.cause());
                    }
                }
            };

            // Prepare codec.
            Codec<Object> codec = codecFactory.createCodec();

            NettyClientStateHandler<T> stateHandler = new NettyClientStateHandler<>(this, callback);

            // Prepare channel handlers.
            bootstrap.handler(new ChannelInitializer() {
                @Override
                protected void initChannel(Channel ch) throws Exception {
                    ChannelPipeline pipeline = ch.pipeline();

                    NettyClient<T> client = NettyClient.this;

                    NettyClientHandler<T> msgHandler = new NettyClientHandler<>(id(), localEpoch, protocol, login, connectTimeout,
                        idleTimeout, log, metrics, client, callback);

                    NettyClientDeferHandler<T> deferHandler = new NettyClientDeferHandler<>(id(), log);

                    NetworkProtocolCodec netCodec = new NetworkProtocolCodec(codec);

                    if (metrics != null) {
                        pipeline.addLast(new ChannelTrafficShapingHandler(0, 0, TRAFFIC_SHAPING_INTERVAL) {
                            @Override
                            protected void doAccounting(TrafficCounter counter) {
                                metrics.onBytesReceived(counter.lastReadBytes());
                                metrics.onBytesSent(counter.lastWrittenBytes());
                            }
                        });
                    }

                    pipeline.addLast(DECODER_HANDLER_ID, netCodec.getDecoder());
                    pipeline.addLast(ENCODER_HANDLER_ID, netCodec.getEncoder());

                    pipeline.addLast(msgHandler);
                    pipeline.addLast(NettyClientDeferHandler.class.getName(), deferHandler);
                    pipeline.addLast(stateHandler);
                }
            });

            // Connect channel.
            ChannelFuture future = bootstrap.connect();

            // Register state handler as a listener after lock is released.
            afterLock = () -> future.addListener(stateHandler);

            Channel channel = future.channel();

            channelCtx = new ChannelContext(channel, allWritesListener, codec);
        } finally {
            lock.unlock();
        }

        afterLock.run();

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
            log.debug("Sending message [channel={}, message={}]", id(), msg);
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
                log.debug("Pre-encoding message [channel={}, message={}]", id(), msg);
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
                    log.debug("Done sending [channel={}, message={}]", id(), msg);
                } else {
                    log.debug("Failed to send message [channel={}, message={}]", id(), msg, result.cause());
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
            log.error("Failed to notify callback on network operation failure [channel={}, message={}]", id(), msg, e);
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
                log.trace("Setting option {} = {} [channel={}]", opt, value, id());
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

    private String id() {
        return protocol + ':' + address;
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
