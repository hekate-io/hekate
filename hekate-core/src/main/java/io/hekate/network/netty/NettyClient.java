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

package io.hekate.network.netty;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecException;
import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkSendCallback;
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
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.traffic.ChannelTrafficShapingHandler;
import io.netty.handler.traffic.TrafficCounter;
import io.netty.util.concurrent.GenericFutureListener;
import io.netty.util.internal.ThrowableUtil;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.network.NetworkClient.State.CONNECTED;
import static io.hekate.network.NetworkClient.State.CONNECTING;
import static io.hekate.network.NetworkClient.State.DISCONNECTED;
import static io.hekate.network.NetworkClient.State.DISCONNECTING;

class NettyClient<T> implements NetworkClient<T>, NettyChannelSupport {
    private static class ChannelContext {
        private final Channel channel;

        private final GenericFutureListener<ChannelFuture> writeListener;

        private final Codec<Object> codec;

        public ChannelContext(Channel channel, GenericFutureListener<ChannelFuture> writeListener, Codec<Object> codec) {
            this.channel = channel;
            this.writeListener = writeListener;
            this.codec = codec;
        }

        public Channel channel() {
            return channel;
        }

        public Codec<Object> codec() {
            return codec;
        }

        public GenericFutureListener<ChannelFuture> writeListener() {
            return writeListener;
        }

        public boolean supportedType(Object msg) {
            return codec.baseType().isInstance(msg);
        }
    }

    private class NettyClientStateHandler<V> extends ChannelInboundHandlerAdapter implements ChannelFutureListener {
        private final String id;

        private final NettyClient<V> client;

        private final int localEpoch;

        private final NetworkFuture<V> epochConnFuture;

        private final NetworkFuture<V> epochDiscFuture;

        private final NetworkClientCallback<V> callback;

        private Throwable firstError;

        private boolean disconnected;

        private boolean connectComplete;

        public NettyClientStateHandler(NettyClient<V> client, NetworkClientCallback<V> callback) {
            this.client = client;
            this.id = client.id();
            this.localEpoch = client.epoch;
            this.epochConnFuture = client.connFuture;
            this.epochDiscFuture = client.discFuture;
            this.callback = callback;
        }

        @Override
        public void operationComplete(ChannelFuture future) throws Exception {
            connectComplete = true;

            if (future.isSuccess()) {
                if (future.channel().isOpen()) {
                    if (trace) {
                        log.trace("Channel connect future completed successfully [to={}]", id);
                    }
                } else {
                    // Channel was disconnect()'ed while we were connecting.
                    becomeDisconnected();
                }
            } else if (firstError == null) {
                if (trace) {
                    log.trace("Notifying on connect future failure [to={}]", id, future.cause());
                }

                firstError = NettyErrorUtils.unwrap(future.cause());

                ChannelPipeline pipeline = future.channel().pipeline();

                if (pipeline.names().contains(NettyClientStateHandler.class.getName())) {
                    pipeline.fireExceptionCaught(firstError);
                } else {
                    becomeDisconnected();
                }
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            localAddress = (InetSocketAddress)ctx.channel().localAddress();

            if (trace) {
                log.debug("Channel is active [to={}]", id);
            }

            super.channelActive(ctx);
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            if (evt instanceof HandshakeDoneEvent) {
                HandshakeDoneEvent handshake = (HandshakeDoneEvent)evt;

                if (trace) {
                    log.trace("Processing handshake event [to={}, event={}]", id, evt);
                }

                // Try to update state and decide on whether the user callback should be notified.
                boolean notify = withLock(() -> {
                    if (handshake.epoch() == client.epoch && state == CONNECTING) {
                        if (debug) {
                            log.debug("Updated connection state [old={}, new={}, to={}]", state, CONNECTED, id);
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

                    epochConnFuture.complete(client);
                } else {
                    if (trace) {
                        log.trace("Skipped processing of handshake event [to={}, event={}]", id, evt);
                    }
                }
            }

            // Check that channel wasn't closed by the callback.
            if (ctx.channel().isOpen()) {
                ctx.fireUserEventTriggered(evt);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable error) throws Exception {
            if (firstError != null) {
                // Handle only the very first error.
                return;
            }

            // Propagate exception through the pipeline if there are some other handlers.
            if (ctx.pipeline().last() != this) {
                ctx.fireExceptionCaught(error);
            }

            if (error instanceof CodecException) {
                // Ignore since this is an application-level error.
                return;
            }

            if (trace) {
                log.trace("Exception caught in state handler [to={}]", id, error);
            }

            firstError = NettyErrorUtils.unwrap(error);

            ctx.close();
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            if (trace) {
                log.trace("Channel unregistered [to={}]", id);
            }

            super.channelUnregistered(ctx);

            becomeDisconnected();
        }

        private void becomeDisconnected() {
            if (!connectComplete) {
                if (trace) {
                    log.trace("Skipped on-disconnect notification since connect is not complete yet [to={}, state={}]", id, state);
                }
            } else if (disconnected) {
                if (trace) {
                    log.trace("Skipped on-disconnect notification since already disconnected [to={}, state={}]", id, state);
                }
            } else {
                disconnected = true;

                if (firstError != null) {
                    if (NettyErrorUtils.isNonFatalIoError(firstError)) {
                        if (debug) {
                            log.debug("Closing outbound connection due to I/O error "
                                + "[to={}, state={}, cause={}]", id, state, firstError.toString());
                        }
                    } else if (log.isErrorEnabled()) {
                        log.error("Outbound connection failure [to={}, state={}]", id, state, firstError);
                    }
                }

                boolean ignoreError = withLock(() -> {
                    if (localEpoch == client.epoch && state != DISCONNECTED) {
                        if (debug) {
                            log.debug("Updated connection state [old={}, new={}, to={}]", state, DISCONNECTED, id);
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
                    log.trace("Notifying callbacks on disconnect [to={}, error={}]", id, finalError);
                }

                callback.onDisconnect(client, finalError);

                if (finalError.isPresent()) {
                    epochConnFuture.completeExceptionally(finalError.get());
                } else {
                    epochConnFuture.complete(client);
                }

                epochDiscFuture.complete(client);
            }
        }
    }

    static final int TRAFFIC_SHAPING_INTERVAL = 1000;

    static final String ENCODER_HANDLER_ID = "encoder";

    private static final ClosedChannelException WRITE_CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new ClosedChannelException(), NettyClient.class, "doSend()"
    );

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

    private final NettyMetricsSink metrics;

    private final Logger log;

    private final boolean debug;

    private final boolean trace;

    private final boolean epoll;

    private final EventLoop eventLoop;

    private final int affinity = ThreadLocalRandom.current().nextInt();

    private final NettyWriteQueue writeQueue;

    private final SslContext ssl;

    private NetworkFuture<T> connFuture;

    private NetworkFuture<T> discFuture;

    private int epoch;

    private State state = DISCONNECTED;

    private volatile ChannelContext channelCtx;

    private volatile Object userCtx;

    private volatile InetSocketAddress remoteAddress;

    private volatile InetSocketAddress localAddress;

    @SuppressWarnings("unchecked")
    public NettyClient(NettyClientFactory<T> factory) {
        assert factory != null : "Configuration is null.";

        ConfigCheck check = ConfigCheck.get(NettyClientFactory.class);

        check.notEmpty(factory.getProtocol(), "protocol");
        check.validSysName(factory.getProtocol(), "protocol");
        check.notNull(factory.getCodecFactory(), "codec factory");
        check.notNull(factory.getEventLoop(), "event loops group");

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
        epoll = factory.getEventLoop() instanceof EpollEventLoopGroup;
        eventLoop = factory.getEventLoop().next();
        metrics = factory.getMetrics();
        ssl = factory.getSsl();

        writeQueue = new NettyWriteQueue(factory.getSpy());
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        return remoteAddress;
    }

    @Override
    public InetSocketAddress localAddress() {
        return localAddress;
    }

    @Override
    public boolean isSecure() {
        return ssl != null;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <C> C getContext() {
        return (C)userCtx;
    }

    @Override
    public void setContext(Object ctx) {
        this.userCtx = ctx;
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
    public State state() {
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

        return localCtx != null && localCtx.channel().config().isAutoRead();
    }

    @Override
    public NetworkFuture<T> disconnect() {
        return withLock(() -> {
            if (state == DISCONNECTING) {
                // Already disconnecting.
                return discFuture;
            } else if (state == CONNECTING || state == CONNECTED) {
                // Update state.
                if (debug) {
                    log.debug("Updated connection state [old={}, new={}, to={}]", state, DISCONNECTING, id());
                }

                state = DISCONNECTING;

                if (trace) {
                    log.trace("Invoking close [to={}", id());
                }

                channelCtx.channel().close();

                channelCtx = null;

                return discFuture;
            } else {
                // Not connected.
                if (trace) {
                    log.trace("Skipped disconnect request since client is already in {} state.", state);
                }

                if (discFuture == null) {
                    return NetworkFuture.completed(this);
                } else {
                    return discFuture;
                }
            }
        });
    }

    @Override
    public Optional<Channel> nettyChannel() {
        ChannelContext channelCtx = this.channelCtx;

        return channelCtx != null ? Optional.of(channelCtx.channel) : Optional.empty();
    }

    private void pauseReceiver(boolean pause, Consumer<NetworkEndpoint<T>> callback) {
        ChannelContext localCtx = this.channelCtx;

        if (localCtx != null) {
            if (debug) {
                if (pause) {
                    log.debug("Pausing outbound receiver [to={}]", id());
                } else {
                    log.debug("Resuming outbound receiver [to={}]", id());
                }
            }

            Channel channel = localCtx.channel();
            EventLoop eventLoop = channel.eventLoop();

            if (eventLoop.inEventLoop()) {
                channel.config().setAutoRead(!pause);

                notifyOnReceivePause(pause, callback, channel);
            } else {
                eventLoop.execute(() -> {
                    channel.config().setAutoRead(!pause);

                    notifyOnReceivePause(pause, callback, channel);
                });
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
                    + "[pause={}, to={}]", pause, id(), e);
            }
        }
    }

    private NetworkFuture<T> doConnect(boolean required, InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        ArgAssert.notNull(address, "Address");
        ArgAssert.notNull(callback, "Callback");

        NetworkFuture<T> localConnFuture;
        Runnable afterLock;

        lock.lock();

        try {
            if (state == CONNECTING || state == CONNECTED) {
                if (required) {
                    throw new IllegalStateException("Client is in " + state + " state [address=" + address + ']');
                } else {
                    return connFuture;
                }
            }

            if (eventLoop.isTerminated()) {
                throw new IllegalStateException("I/O thread pool terminated.");
            }

            this.remoteAddress = address;

            // Prepare Netty bootstrap.
            Bootstrap bootstrap = new Bootstrap();

            if (epoll) {
                if (debug) {
                    log.debug("Connecting [to={}, transport=EPOLL]", id());
                }

                bootstrap.channel(EpollSocketChannel.class);
            } else {
                if (debug) {
                    log.debug("Connecting [to={}, transport=NIO]", id());
                }

                bootstrap.channel(NioSocketChannel.class);
            }

            bootstrap.group(eventLoop);
            bootstrap.remoteAddress(address);

            // Apply configuration options.
            setOpts(bootstrap);

            // Increment connection epoch (set to 1 if we reached max int value).
            int localEpoch = this.epoch = epoch < Integer.MAX_VALUE ? epoch + 1 : 1;

            // Prepare connect/disconnect future.
            connFuture = localConnFuture = new NetworkFuture<>();
            discFuture = new NetworkFuture<>();

            // Update state.
            state = CONNECTING;

            // Prepare common listener that should be attached to all write operations.
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
                    NettyClient<T> client = NettyClient.this;

                    NettyClientHandler<T> msgHandler = new NettyClientHandler<>(id(), localEpoch, protocol, affinity, login,
                        connectTimeout, idleTimeout, log, metrics, client, callback);

                    NettyClientDeferHandler deferHandler = new NettyClientDeferHandler(id(), log);

                    NetworkProtocolCodec netCodec = new NetworkProtocolCodec(codec);

                    ChannelPipeline pipe = ch.pipeline();

                    if (ssl != null) {
                        SslHandler sslHandler = ssl.newHandler(ch.alloc(), AddressUtils.host(address), address.getPort());

                        if (connectTimeout != null && connectTimeout > 0) {
                            sslHandler.setHandshakeTimeoutMillis(connectTimeout);
                        }

                        pipe.addLast(sslHandler);
                    }

                    if (metrics != null) {
                        pipe.addLast(new ChannelTrafficShapingHandler(0, 0, TRAFFIC_SHAPING_INTERVAL) {
                            @Override
                            protected void doAccounting(TrafficCounter counter) {
                                metrics.onBytesReceived(counter.lastReadBytes());
                                metrics.onBytesSent(counter.lastWrittenBytes());
                            }
                        });
                    }

                    pipe.addLast(NetworkVersionEncoder.INSTANCE);
                    pipe.addLast(DECODER_HANDLER_ID, netCodec.decoder());
                    pipe.addLast(ENCODER_HANDLER_ID, netCodec.encoder());

                    pipe.addLast(msgHandler);
                    pipe.addLast(NettyClientStateHandler.class.getName(), stateHandler);
                    pipe.addLast(NettyClientDeferHandler.class.getName(), deferHandler);
                }
            });

            // Connect channel.
            ChannelFuture future = bootstrap.connect();

            // Register state handler as a listener after the lock is released.
            afterLock = () -> future.addListener(stateHandler);

            Channel channel = future.channel();

            channelCtx = new ChannelContext(channel, allWritesListener, codec);
        } finally {
            lock.unlock();
        }

        afterLock.run();

        return localConnFuture;
    }

    private void cleanup() {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        channelCtx = null;
        remoteAddress = null;
        localAddress = null;
        connFuture = null;
    }

    private void doSend(T msg, NetworkSendCallback<T> onSend) {
        ChannelContext localCtx = this.channelCtx;

        if (localCtx == null) {
            // Notify on channel close error.
            if (metrics != null) {
                metrics.onMessageSendError();
            }

            if (onSend != null) {
                NettyUtils.runAtAllCost(eventLoop, () ->
                    notifyOnError(msg, onSend, WRITE_CLOSED_CHANNEL_EXCEPTION)
                );
            }
        } else {
            // Write message to the channel.
            write(msg, onSend, localCtx);
        }
    }

    private void write(T msg, NetworkSendCallback<T> onSend, ChannelContext ctx) {
        if (!ctx.supportedType(msg)) {
            String expected = ctx.codec().baseType().getName();
            String real = msg.getClass().getName();

            notifyOnError(msg, onSend, new CodecException("Unsupported message type [expected=" + expected + ", real=" + real + ']'));
        }

        if (debug) {
            log.debug("Sending message [to={}, message={}]", id(), msg);
        }

        Channel channel = ctx.channel();
        Codec<Object> codec = ctx.codec();

        // Update metrics.
        if (metrics != null) {
            metrics.onMessageEnqueue();
        }

        boolean failed = false;

        // Prepare deferred message.
        DeferredMessage deferred;

        // Maybe pre-encode message.
        if (codec.isStateful()) {
            deferred = new DeferredMessage(msg, msg, channel);
        } else {
            if (debug) {
                log.debug("Pre-encoding message [to={}, message={}]", id(), msg);
            }

            try {
                ByteBuf buf = NetworkProtocolCodec.preEncode(msg, codec, channel.alloc());

                deferred = new DeferredMessage(buf, msg, channel);
            } catch (CodecException e) {
                deferred = fail(msg, channel, e);

                failed = true;
            }
        }

        // Register listener.
        deferred.addListener((ChannelFuture result) -> {
            if (debug) {
                if (result.isSuccess()) {
                    log.debug("Done sending [to={}, message={}]", id(), msg);
                } else {
                    log.debug("Failed to send message [to={}, message={}]", id(), msg, result.cause());
                }
            }

            // Notify common listener.
            ctx.writeListener().operationComplete(result);

            // Notify user callback.
            if (onSend != null) {
                Throwable mayBeError = NettyErrorUtils.unwrap(result.cause());

                onSend.onComplete(msg, Optional.ofNullable(mayBeError), this);
            }
        });

        // Enqueue write operation.
        if (!failed) {
            writeQueue.enqueue(deferred, eventLoop);
        }
    }

    private DeferredMessage fail(T msg, Channel channel, Throwable error) {
        DeferredMessage promise = new DeferredMessage(msg, msg, channel);

        promise.setFailure(error);

        return promise;
    }

    private void notifyOnError(T msg, NetworkSendCallback<T> onSend, Throwable error) {
        try {
            onSend.onComplete(msg, Optional.of(error), this);
        } catch (RuntimeException | Error e) {
            log.error("Failed to notify callback on network operation failure [to={}, message={}]", id(), msg, e);
        }
    }

    private void setOpts(Bootstrap bootstrap) {
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        setUserOpt(bootstrap, ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        setUserOpt(bootstrap, ChannelOption.TCP_NODELAY, tcpNoDelay);
        setUserOpt(bootstrap, ChannelOption.SO_RCVBUF, soReceiveBufferSize);
        setUserOpt(bootstrap, ChannelOption.SO_SNDBUF, soSendBufferSize);
        setUserOpt(bootstrap, ChannelOption.SO_REUSEADDR, soReuseAddress);
    }

    private <O> void setUserOpt(Bootstrap bootstrap, ChannelOption<O> opt, O value) {
        if (value != null) {
            if (trace) {
                log.trace("Setting option {} = {} [to={}]", opt, value, id());
            }

            bootstrap.option(opt, value);
        }
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
        return protocol + ':' + remoteAddress;
    }

    @Override
    public String toString() {
        State localState;
        InetSocketAddress localAddr;

        lock.lock();

        try {
            localState = state;
            localAddr = remoteAddress;
        } finally {
            lock.unlock();
        }

        return getClass().getSimpleName() + "["
            + "protocol=" + protocol
            + ", to=" + localAddr
            + ", state=" + localState
            + ']';
    }
}
