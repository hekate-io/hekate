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

import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkServerCallback;
import io.hekate.network.NetworkServerFailure;
import io.hekate.network.NetworkServerFailure.Resolution;
import io.hekate.network.NetworkServerFuture;
import io.hekate.network.NetworkServerHandlerConfig;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.network.NetworkServer.State.STARTED;
import static io.hekate.network.NetworkServer.State.STARTING;
import static io.hekate.network.NetworkServer.State.STOPPED;
import static io.hekate.network.NetworkServer.State.STOPPING;

class DefaultNettyServer implements NettyServer, NettyChannelSupport {
    static class HandlerRegistration {
        private final NettyServerHandlerConfig<Object> config;

        private final Map<NettyServerClient, Void> clients = new IdentityHashMap<>();

        // Volatile since it can be read and updated by different threads out of synchronized context.
        private volatile NettyMetricsSink metrics;

        public HandlerRegistration(NettyServerHandlerConfig<Object> config, NettyMetricsSink metrics) {
            this.config = config;
            this.metrics = metrics;
        }

        public NettyServerHandlerConfig<Object> config() {
            return config;
        }

        public NettyMetricsSink metrics() {
            return metrics;
        }

        public List<NetworkEndpoint<?>> clients() {
            synchronized (clients) {
                if (clients.isEmpty()) {
                    return Collections.emptyList();
                } else {
                    return new ArrayList<>(clients.keySet());
                }
            }
        }

        public void add(NettyServerClient client) {
            synchronized (clients) {
                clients.put(client, null);
            }
        }

        public void remove(NettyServerClient client) {
            synchronized (clients) {
                clients.remove(client, null);
            }
        }

        public void setMetrics(NettyMetricsSink metrics) {
            this.metrics = metrics;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultNettyServer.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final ReentrantLock lock = new ReentrantLock();

    private final boolean autoAccept;

    private final int hbInterval;

    private final int hbLossThreshold;

    private final boolean hbDisabled;

    private final boolean tcpNoDelay;

    private final Integer soReceiveBufferSize;

    private final Integer soSendBuffer;

    private final Boolean soReuseAddress;

    private final Integer soBacklog;

    private final SslContext ssl;

    private final Map<String, CodecFactory<Object>> codecs = Collections.synchronizedMap(new HashMap<>());

    private final Map<String, HandlerRegistration> handlers = new ConcurrentHashMap<>();

    private final Map<SocketChannel, NettyServerClient> clients = new IdentityHashMap<>();

    private final EventLoopGroup acceptors;

    private final EventLoopGroup workers;

    private NettyMetricsFactory metrics;

    private Channel server;

    private NetworkServerCallback callback;

    private NetworkServerFuture startFuture;

    private boolean failoverInProgress;

    private NetworkServerFuture stopFuture;

    // Volatile since address is updated (to reflect a real port) after server is started and can be accessed in non-synchronized context.
    private volatile InetSocketAddress address;
    // Volatile since it can be accessed in non-synchronized context.

    private volatile State state = STOPPED;

    public DefaultNettyServer(NettyServerFactory factory) {
        ArgAssert.notNull(factory, "Factory");

        ConfigCheck check = ConfigCheck.get(NettyServerFactory.class);

        check.notNull(factory.getAcceptorEventLoop(), "acceptor event loop");
        check.notNull(factory.getWorkerEventLoop(), "worker event loop");

        autoAccept = factory.isAutoAccept();
        hbInterval = factory.getHeartbeatInterval();
        hbLossThreshold = factory.getHeartbeatLossThreshold();
        hbDisabled = factory.isDisableHeartbeats();
        tcpNoDelay = factory.isTcpNoDelay();
        soReceiveBufferSize = factory.getSoReceiveBufferSize();
        soSendBuffer = factory.getSoSendBufferSize();
        soReuseAddress = factory.getSoReuseAddress();
        soBacklog = factory.getSoBacklog();
        ssl = factory.getSsl();
        metrics = factory.getMetrics();

        acceptors = factory.getAcceptorEventLoop();
        workers = factory.getWorkerEventLoop();

        checkWorkerEventLoopType(check, workers);

        if (factory.getHandlers() != null) {
            factory.getHandlers().forEach(this::addHandler);
        }
    }

    @Override
    public InetSocketAddress address() {
        return address;
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public NetworkServerFuture start(InetSocketAddress bindAddress) {
        return start(bindAddress, null);
    }

    @Override
    public NetworkServerFuture start(InetSocketAddress address, NetworkServerCallback callback) {
        ArgAssert.notNull(address, "Address");

        lock.lock();

        try {
            if (state != STOPPED) {
                throw new IllegalStateException("Server is in " + state + " state [address=" + this.address + ']');
            }

            if (DEBUG) {
                log.debug("Starting [address={}]", this.address);
            }

            this.state = STARTING;
            this.address = address;
            this.callback = callback;

            startFuture = new NetworkServerFuture();

            doStart(0);

            return startFuture;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void startAccepting() {
        lock.lock();

        try {
            if (server != null && !server.config().isAutoRead()) {
                if (DEBUG) {
                    log.debug("Start accepting [address={}]", address);
                }

                server.config().setAutoRead(true);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public NetworkServerFuture stop() {
        return doStop(null);
    }

    @Override
    public void addHandler(NetworkServerHandlerConfig<?> cfg) {
        @SuppressWarnings("unchecked")
        NetworkServerHandlerConfig<Object> objCfg = (NetworkServerHandlerConfig<Object>)cfg;

        addHandler(copy(objCfg));
    }

    public void addHandler(NettyServerHandlerConfig<?> cfg) {
        lock.lock();

        try {
            ConfigCheck check = validate(cfg);

            @SuppressWarnings("unchecked")
            NettyServerHandlerConfig<Object> nettyCfg = (NettyServerHandlerConfig<Object>)cfg;

            NettyServerHandlerConfig<Object> copy = copy(nettyCfg);

            copy.setEventLoop(cfg.getEventLoop());

            checkWorkerEventLoopType(check, copy.getEventLoop());

            if (DEBUG) {
                log.debug("Adding handler [protocol={}]", copy);
            }

            NettyMetricsSink metricsSink = null;

            if (metrics != null) {
                metricsSink = metrics.createSink(copy.getProtocol());
            }

            HandlerRegistration registration = new HandlerRegistration(copy, metricsSink);

            handlers.put(copy.getProtocol(), registration);

            codecs.put(copy.getProtocol(), copy.getCodecFactory());
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<NetworkEndpoint<?>> removeHandler(String protocol) {
        ArgAssert.notNull(protocol, "Protocol");

        if (DEBUG) {
            log.debug("Removing handler [protocol={}]", protocol);
        }

        lock.lock();

        try {
            handlers.remove(protocol);
            codecs.remove(protocol);

            List<NetworkEndpoint<?>> liveClients = new ArrayList<>();

            for (NettyServerClient client : clients.values()) {
                String clientProtocol = client.protocol();

                if (clientProtocol != null && clientProtocol.equals(protocol)) {
                    liveClients.add(client);
                }
            }

            return liveClients;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<NetworkEndpoint<?>> clients(String protocol) {
        HandlerRegistration handler = handlers.get(protocol);

        if (handler != null) {
            return handler.clients();
        }

        return Collections.emptyList();
    }

    @Override
    public Optional<Channel> nettyChannel() {
        lock.lock();

        try {
            return Optional.ofNullable(server);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void setMetrics(NettyMetricsFactory metrics) {
        lock.lock();

        try {
            this.metrics = metrics;

            handlers.forEach((protocol, handler) ->
                handler.setMetrics(metrics.createSink(protocol))
            );
        } finally {
            lock.unlock();
        }
    }

    private void doStart(int attempt) {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        ServerBootstrap boot = new ServerBootstrap();

        if (acceptors instanceof EpollEventLoopGroup) {
            if (DEBUG) {
                log.debug("Using EPOLL server socket channel.");
            }

            boot.channel(EpollServerSocketChannel.class);
        } else {
            if (DEBUG) {
                log.debug("Using NIO server socket channel.");
            }

            boot.channel(NioServerSocketChannel.class);
        }

        boot.group(acceptors, workers);

        setOpts(boot);
        setChildOpts(boot);

        boot.handler(new ChannelHandlerAdapter() {
            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                tryFailover(ctx.channel(), attempt, cause);
            }
        });

        boot.childHandler(new ChannelInitializer<SocketChannel>() {
            @Override
            protected void initChannel(SocketChannel channel) throws Exception {
                InetSocketAddress remoteAddress = channel.remoteAddress();
                InetSocketAddress localAddress = channel.localAddress();

                lock.lock();

                try {
                    if (state == STOPPING || state == STOPPED) {
                        if (DEBUG) {
                            log.debug("Closing connection since server is in {} state [address={}].", state, remoteAddress);
                        }

                        channel.close();

                        return;
                    }

                    NettyServerClient clientHandler = new NettyServerClient(
                        remoteAddress,
                        localAddress,
                        ssl != null,
                        hbInterval,
                        hbLossThreshold,
                        hbDisabled,
                        handlers,
                        workers
                    );

                    clients.put(channel, clientHandler);

                    NetworkProtocolCodec codec = new NetworkProtocolCodec(codecs);

                    ChannelPipeline pipeline = channel.pipeline();

                    if (ssl != null) {
                        pipeline.addLast(ssl.newHandler(channel.alloc()));
                    }

                    pipeline.addLast(new NetworkVersionDecoder());
                    pipeline.addLast(codec.encoder());
                    pipeline.addLast(codec.decoder());
                    pipeline.addLast(clientHandler);

                    channel.closeFuture().addListener(close -> {
                        if (DEBUG) {
                            log.debug("Removing connection from server registry [address={}]", remoteAddress);
                        }

                        lock.lock();

                        try {
                            clients.remove(channel);
                        } finally {
                            lock.unlock();
                        }
                    });
                } finally {
                    lock.unlock();
                }
            }
        });

        ChannelFuture bindFuture = boot.bind(address);

        server = bindFuture.channel();

        bindFuture.addListener((ChannelFutureListener)bind -> {
            if (bind.isSuccess()) {
                lock.lock();

                try {
                    failoverInProgress = false;

                    if (state == STARTING) {
                        state = STARTED;

                        // Updated since port can be automatically assigned by the underlying OS.
                        address = (InetSocketAddress)bind.channel().localAddress();

                        if (DEBUG) {
                            log.debug("Started [address={}]", address);
                        }

                        if (!startFuture.isDone() && callback != null) {
                            callback.onStart(this);
                        }

                        startFuture.complete(this);
                    }
                } finally {
                    lock.unlock();
                }
            } else {
                tryFailover(bind.channel(), attempt, bind.cause());
            }
        });
    }

    private void tryFailover(Channel channel, int attempt, Throwable cause) {
        boolean stopWithError = true;

        if (cause instanceof IOException) {
            lock.lock();

            try {
                if (state == STARTED || state == STARTING) {
                    InetSocketAddress newAddress = null;

                    long delay = 0;

                    if (callback != null) {
                        NetworkServerFailure failure = new NettyServerFailure(cause, attempt, address);

                        try {
                            Resolution resolution = callback.onFailure(this, failure);

                            if (resolution != null && !resolution.isFailure()) {
                                newAddress = resolution.retryAddress();

                                if (newAddress == null) {
                                    // Reuse old address.
                                    newAddress = address;
                                }

                                delay = resolution.retryDelay();
                            }
                        } catch (RuntimeException | Error e) {
                            if (log.isErrorEnabled()) {
                                log.error("Got an unexpected runtime error while notifying network server callback on failure.", e);
                            }
                        }
                    }

                    if (newAddress != null) {
                        if (DEBUG) {
                            log.debug("Network server encountered an I/O error ...will try to restart after {} ms "
                                + "[old-address={}, new-address={}]", delay, address, newAddress, cause);
                        }

                        channel.close();

                        stopWithError = false;

                        failoverInProgress = true;

                        address = newAddress;

                        Runnable failoverTask = () -> {
                            lock.lock();

                            try {
                                if (failoverInProgress) {
                                    failoverInProgress = false;

                                    doStart(attempt + 1);
                                }
                            } catch (RuntimeException | Error e) {
                                if (log.isErrorEnabled()) {
                                    log.error("Got an unexpected runtime error during network server failover.", e);
                                }
                            } finally {
                                lock.unlock();
                            }
                        };

                        if (delay > 0) {
                            acceptors.schedule(failoverTask, delay, TimeUnit.MILLISECONDS);
                        } else {
                            acceptors.submit(failoverTask);
                        }
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        if (stopWithError) {
            if (DEBUG) {
                log.debug("Network server encountered an error and will be stopped [address={}]", address, cause);
            }

            doStop(cause);
        }
    }

    private NetworkServerFuture doStop(Throwable cause) {
        lock.lock();

        try {
            if (state == STOPPING) {
                return stopFuture;
            } else if (state == STARTING || state == STARTED) {
                State oldState = state;

                state = STOPPING;

                failoverInProgress = false;

                if (DEBUG) {
                    log.debug("Stopping [address={}]", address);
                }

                NetworkServerCallback localCallback = this.callback;
                NetworkServerFuture localStartFuture = this.startFuture;
                NetworkServerFuture localStopFuture = this.stopFuture = new NetworkServerFuture();

                server.close().addListener(serverClose -> {
                    CompletableFuture<Void> allClientsClosed = new CompletableFuture<>();

                    lock.lock();

                    try {
                        // Close client connections.
                        if (clients.isEmpty()) {
                            allClientsClosed.complete(null);
                        } else {
                            List<SocketChannel> connectionsCopy = new ArrayList<>(clients.keySet());

                            clients.clear();

                            AtomicInteger remaining = new AtomicInteger(connectionsCopy.size());

                            connectionsCopy.forEach(channel -> {
                                if (DEBUG) {
                                    log.debug("Closing connection due to server shutdown [address={}]", channel.remoteAddress());
                                }

                                channel.close().addListener(clientClose -> {
                                    if (remaining.decrementAndGet() == 0) {
                                        allClientsClosed.complete(null);
                                    }
                                });
                            });
                        }
                    } finally {
                        lock.unlock();
                    }

                    allClientsClosed.thenRun(() -> {
                        lock.lock();

                        try {
                            state = STOPPED;

                            server = null;

                            if (oldState == STARTED && localCallback != null && !localStopFuture.isDone()) {
                                localCallback.onStop(this);
                            }

                            if (oldState == STARTING && cause != null) {
                                localStartFuture.completeExceptionally(cause);
                            } else {
                                localStartFuture.complete(this);
                            }

                            if (DEBUG) {
                                log.debug("Stopped [address={}]", address);
                            }

                            localStopFuture.complete(this);

                            // Cleanup fields only if they were not changed by a concurrent start.

                            // JVM identity check to make sure that objects are exactly the same.
                            if (startFuture == localStartFuture) {
                                startFuture = null;
                            }

                            // JVM identity check to make sure that objects are exactly the same.
                            if (stopFuture == localStopFuture) {
                                stopFuture = null;
                            }

                            // JVM identity check to make sure that objects are exactly the same.
                            if (callback == localCallback) {
                                callback = null;
                            }
                        } finally {
                            lock.unlock();
                        }
                    });
                });

                return localStopFuture;
            } else {
                if (DEBUG) {
                    log.debug("Skipped stop request since server is in {} state [address={}]", state, address);
                }
            }
        } finally {
            lock.unlock();
        }

        return NetworkServerFuture.completed(this);
    }

    private NettyServerHandlerConfig<Object> copy(NetworkServerHandlerConfig<Object> source) {
        NettyServerHandlerConfig<Object> copy = new NettyServerHandlerConfig<>();

        copy.setProtocol(source.getProtocol());
        copy.setHandler(source.getHandler());
        copy.setCodecFactory(source.getCodecFactory());
        copy.setLoggerCategory(source.getLoggerCategory());

        return copy;
    }

    private ConfigCheck validate(NetworkServerHandlerConfig<?> handler) {
        assert lock.isHeldByCurrentThread() : "Thread must hold lock.";

        ConfigCheck check = ConfigCheck.get(NetworkServerHandlerConfig.class);

        check.notEmpty(handler.getProtocol(), "protocol");
        check.validSysName(handler.getProtocol(), "protocol");
        check.unique(handler.getProtocol(), handlers.keySet(), "protocol");
        check.notNull(handler.getHandler(), "handler");
        check.notNull(handler.getCodecFactory() != null, "codec factory");

        return check;
    }

    private void setChildOpts(ServerBootstrap boot) {
        boot.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

        setChildUserOpt(boot, ChannelOption.TCP_NODELAY, tcpNoDelay);
        setChildUserOpt(boot, ChannelOption.SO_RCVBUF, soReceiveBufferSize);
        setChildUserOpt(boot, ChannelOption.SO_SNDBUF, soSendBuffer);
    }

    private void setOpts(ServerBootstrap boot) {
        setUserOpt(boot, ChannelOption.SO_BACKLOG, soBacklog);
        setUserOpt(boot, ChannelOption.SO_RCVBUF, soReceiveBufferSize);
        setUserOpt(boot, ChannelOption.SO_REUSEADDR, soReuseAddress);

        if (!autoAccept) {
            setUserOpt(boot, ChannelOption.AUTO_READ, false);
        }
    }

    private <O> void setChildUserOpt(ServerBootstrap boot, ChannelOption<O> opt, O value) {
        if (value != null) {
            if (DEBUG) {
                log.debug("Setting option {} = {} [address={}]", opt, value, address);
            }

            boot.childOption(opt, value);
        }
    }

    private <O> void setUserOpt(ServerBootstrap boot, ChannelOption<O> opt, O value) {
        if (value != null) {
            if (DEBUG) {
                log.debug("Setting option {} = {} [address={}]", opt, value, address);
            }

            boot.option(opt, value);
        }
    }

    private void checkWorkerEventLoopType(ConfigCheck check, EventLoopGroup group) {
        if (group != null) {
            check.isTrue(acceptors.getClass().isAssignableFrom(group.getClass()), "Can't mix different types of event loop groups "
                + "[acceptors=" + acceptors.getClass().getName() + ", workers=" + group.getClass().getName() + ']');
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName()
            + "[address=" + address
            + ", state=" + state
            + ", handlers=" + handlers.keySet()
            + ']';
    }
}
