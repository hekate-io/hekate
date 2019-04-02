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

package io.hekate.network.internal;

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.codec.SingletonCodecFactory;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.resource.ResourceService;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.NetworkBindCallback;
import io.hekate.core.service.NetworkServiceManager;
import io.hekate.core.service.TerminatingService;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkPingCallback;
import io.hekate.network.NetworkPingResult;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerCallback;
import io.hekate.network.NetworkServerFailure;
import io.hekate.network.NetworkServerFuture;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.NetworkServiceJmx;
import io.hekate.network.NetworkSslConfig;
import io.hekate.network.NetworkTimeoutException;
import io.hekate.network.NetworkTransportType;
import io.hekate.network.address.AddressSelector;
import io.hekate.network.netty.NettyClientFactory;
import io.hekate.network.netty.NettyServerFactory;
import io.hekate.network.netty.NettyServerHandlerConfig;
import io.hekate.network.netty.NettyUtils;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

public class NettyNetworkService implements NetworkService, NetworkServiceManager, DependentService, ConfigurableService,
    InitializingService, TerminatingService, JmxSupport<NetworkServiceJmx> {
    private static class ConnectorRegistration<T> {
        private final EventLoopGroup eventLoop;

        private final NetworkConnector<T> connector;

        private final NettyServerHandlerConfig<T> serverHandler;

        public ConnectorRegistration(EventLoopGroup eventLoop, NetworkConnector<T> connector, NettyServerHandlerConfig<T> serverHandler) {
            assert connector != null : "Connector is null.";

            this.eventLoop = eventLoop;
            this.connector = connector;
            this.serverHandler = serverHandler;
        }

        public boolean hasEventLoop() {
            return eventLoop != null;
        }

        public EventLoopGroup eventLoop() {
            return eventLoop;
        }

        public NettyServerHandlerConfig<T> serverHandler() {
            return serverHandler;
        }

        public NetworkConnector<T> connector() {
            return connector;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(NettyNetworkService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final int NIO_ACCEPTOR_THREADS = 1;

    private static final String PING_PROTOCOL = "hekate.ping";

    private final AddressSelector addressSelector;

    private final int initPort;

    private final int portRange;

    private final int connectTimeout;

    private final long acceptorRetryInterval;

    private final int heartbeatInterval;

    private final int heartbeatLossThreshold;

    private final int nioThreadPoolSize;

    private final NetworkTransportType transport;

    private final boolean tcpNoDelay;

    private final Integer soReceiveBufferSize;

    private final Integer soSendBufferSize;

    private final Boolean soReuseAddress;

    private final Integer soBacklog;

    private final NetworkSslConfig sslConfig;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(NetworkService.class);

    @ToStringIgnore
    private final List<NetworkConnectorConfig<?>> connectorConfigs = new ArrayList<>();

    @ToStringIgnore
    private final Map<String, ConnectorRegistration<?>> connectors = new HashMap<>();

    @ToStringIgnore
    private SslContext clientSsl;

    @ToStringIgnore
    private SslContext serverSsl;

    @ToStringIgnore
    private CodecService codec;

    @ToStringIgnore
    private ResourceService resources;

    @ToStringIgnore
    private JmxService jmx;

    @ToStringIgnore
    private NettyMetricsBuilder metrics;

    @ToStringIgnore
    private EventLoopGroup acceptorLoop;

    @ToStringIgnore
    private EventLoopGroup coreLoop;

    @ToStringIgnore
    private NetworkServer server;

    public NettyNetworkService(NetworkServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(NetworkServiceFactory.class);

        check.range(factory.getPort(), 0, 65535, "port");
        check.notNull(factory.getTransport(), "transport");
        check.notNull(factory.getHostSelector(), "address selector");
        check.positive(factory.getNioThreads(), "NIO thread pool size");
        check.positive(factory.getHeartbeatInterval(), "heartbeat interval");
        check.positive(factory.getHeartbeatLossThreshold(), "heartbeat loss threshold");
        check.positive(factory.getConnectTimeout(), "connect timeout");

        initPort = factory.getPort();
        portRange = factory.getPortRange();
        addressSelector = factory.getHostSelector();
        connectTimeout = factory.getConnectTimeout();
        acceptorRetryInterval = factory.getAcceptRetryInterval();
        heartbeatInterval = factory.getHeartbeatInterval();
        heartbeatLossThreshold = factory.getHeartbeatLossThreshold();
        nioThreadPoolSize = factory.getNioThreads();
        tcpNoDelay = factory.isTcpNoDelay();
        soReceiveBufferSize = factory.getTcpReceiveBufferSize();
        soSendBufferSize = factory.getTcpSendBufferSize();
        soReuseAddress = factory.getTcpReuseAddress();
        soBacklog = factory.getTcpBacklog();
        sslConfig = factory.getSsl();

        if (factory.getTransport() == NetworkTransportType.AUTO) {
            transport = Epoll.isAvailable() ? NetworkTransportType.EPOLL : NetworkTransportType.NIO;
        } else {
            transport = factory.getTransport();
        }

        StreamUtils.nullSafe(factory.getConnectors()).forEach(connectorConfigs::add);

        StreamUtils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            StreamUtils.nullSafe(provider.configureNetwork()).forEach(connectorConfigs::add)
        );

        // Register ping protocol.
        connectorConfigs.add(pingConnector());
    }

    @Override
    public void resolve(DependencyContext ctx) {
        codec = ctx.require(CodecService.class);
        resources = ctx.require(ResourceService.class);
        metrics = new NettyMetricsBuilder(ctx.metrics());

        jmx = ctx.optional(JmxService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        Collection<NetworkConfigProvider> providers = ctx.findComponents(NetworkConfigProvider.class);

        providers.forEach(provider ->
            StreamUtils.nullSafe(provider.configureNetwork()).forEach(connectorConfigs::add)
        );

        if (sslConfig != null) {
            clientSsl = NettySslUtils.clientContext(sslConfig, resources);
            serverSsl = NettySslUtils.serverContext(sslConfig, resources);
        }
    }

    @Override
    public NetworkServerFuture bind(NetworkBindCallback callback) throws HekateException {
        ArgAssert.notNull(callback, "Callback");

        if (DEBUG) {
            log.debug("Obtaining preferred host address...");
        }

        InetAddress publicIp = ipOnly(addressSelector.select());

        if (publicIp == null) {
            throw new HekateException("Failed to select public host address [selector=" + addressSelector + ']');
        }

        if (log.isInfoEnabled()) {
            log.info("Selected public address [address={}]", publicIp);

            log.info("Binding network acceptor [port={}]", initPort);
        }

        guard.lockWrite();

        try {
            guard.becomeInitializing();

            // Prepare event loops.
            acceptorLoop = newEventLoop(NIO_ACCEPTOR_THREADS, "NioAcceptor");
            coreLoop = newEventLoop(nioThreadPoolSize, "NioWorker-core");

            // Prepare server factory.
            NettyServerFactory factory = new NettyServerFactory();

            factory.setAutoAccept(false);
            factory.setHeartbeatInterval(heartbeatInterval);
            factory.setHeartbeatLossThreshold(heartbeatLossThreshold);
            factory.setSoBacklog(soBacklog);
            factory.setSoReceiveBufferSize(soReceiveBufferSize);
            factory.setSoSendBufferSize(soSendBufferSize);
            factory.setSoReuseAddress(soReuseAddress);
            factory.setTcpNoDelay(tcpNoDelay);
            factory.setAcceptorEventLoop(acceptorLoop);
            factory.setWorkerEventLoop(coreLoop);
            factory.setSsl(serverSsl);
            factory.setMetrics(metrics.createServerFactory());

            server = factory.createServer();

            // Using wildcard address.
            InetSocketAddress wildcard = new InetSocketAddress(initPort);

            return server.start(wildcard, new NetworkServerCallback() {
                @Override
                public void onStart(NetworkServer server) {
                    InetSocketAddress realAddress = server.address();

                    if (log.isInfoEnabled()) {
                        log.info("Done binding [bind-address={}]", realAddress);
                    }

                    // Convert to public address (port can be assigned by the auto-increment or by OS if it was configured as 0).
                    InetSocketAddress address = new InetSocketAddress(publicIp, realAddress.getPort());

                    callback.onBind(address);
                }

                @Override
                public NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure err) {
                    Throwable cause = err.cause();

                    // Retry only in case of IO errors.
                    if (cause instanceof IOException) {
                        int initPort = wildcard.getPort();

                        if (initPort > 0 && portRange > 0 && server.state() == NetworkServer.State.STARTING) {
                            // Try to increment the port.
                            int prevPort = err.lastTriedAddress().getPort();

                            int newPort = prevPort + 1;

                            if (newPort < initPort + portRange) {
                                InetSocketAddress newAddress = new InetSocketAddress(err.lastTriedAddress().getAddress(), newPort);

                                if (log.isInfoEnabled()) {
                                    log.info("Couldn't bind on port {} ...will try next port [new-address={}]", prevPort, newAddress);
                                }

                                // Retry with the next port.
                                return err.retry().withRetryAddress(newAddress);
                            }
                        } else if (server.state() == NetworkServer.State.STARTED && acceptorRetryInterval > 0) {
                            if (log.isErrorEnabled()) {
                                log.error("Network server encountered an error ...will try to restart after {} ms [attempt={}, address={}]",
                                    acceptorRetryInterval, err.attempt(), err.lastTriedAddress(), cause);
                            }

                            // Failed while server had already been running for a while -> Retry with the same port.
                            return err.retry().withRetryDelay(acceptorRetryInterval);
                        }
                    }

                    // Fail.
                    return callback.onFailure(err);
                }
            });
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            // Register connectors.
            connectorConfigs.forEach(cfg -> {
                ConnectorRegistration<?> reg = register(cfg);

                if (reg.serverHandler() != null) {
                    server.addHandler(reg.serverHandler());
                }
            });

            // Register JMX (optional).
            if (jmx != null) {
                jmx.register(this);

                for (ConnectorRegistration<?> conn : connectors.values()) {
                    jmx.register(conn.connector(), conn.connector().protocol());
                }
            }

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void preInitialize(InitializationContext ctx) {
        // No-op.
    }

    @Override
    public void postInitialize(InitializationContext ctx) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (log.isInfoEnabled()) {
                    log.info("Started accepting network connections [address={}]", server.address());
                }

                server.startAccepting();
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void preTerminate() {
        // No-op.
    }

    @Override
    public void terminate() {
        // No-op.
    }

    @Override
    public void postTerminate() {
        NetworkServer localServer = null;
        EventLoopGroup localAcceptorLoop = null;
        EventLoopGroup localCoreLoop = null;
        List<EventLoopGroup> localLoops = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                localServer = this.server;
                localAcceptorLoop = this.acceptorLoop;
                localCoreLoop = this.coreLoop;

                localLoops = connectors.values().stream()
                    .filter(ConnectorRegistration::hasEventLoop)
                    .map(ConnectorRegistration::eventLoop)
                    .collect(toList());

                connectors.clear();

                acceptorLoop = null;
                coreLoop = null;
                server = null;
            }
        } finally {
            guard.unlockWrite();
        }

        if (localServer != null) {
            try {
                AsyncUtils.getUninterruptedly(localServer.stop());
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException) {
                    if (DEBUG) {
                        log.debug("Failed to stop network server due to an I/O error [cause={}]", e.toString());
                    }
                } else {
                    log.warn("Failed to stop network server.", cause);
                }
            }
        }

        shutdown(localAcceptorLoop);
        shutdown(localCoreLoop);

        if (localLoops != null) {
            localLoops.forEach(this::shutdown);
        }
    }

    @Override
    public <T> NetworkConnector<T> connector(String protocol) throws IllegalArgumentException {
        ArgAssert.notNull(protocol, "Protocol");

        guard.lockReadWithStateCheck();

        try {
            ConnectorRegistration<?> module = connectors.get(protocol);

            ArgAssert.check(module != null, "Unknown protocol [name=" + protocol + ']');

            @SuppressWarnings("unchecked")
            NetworkConnector<T> connector = (NetworkConnector<T>)module.connector();

            return connector;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public boolean hasConnector(String protocol) {
        guard.lockReadWithStateCheck();

        try {
            return connectors.containsKey(protocol);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void ping(InetSocketAddress address, NetworkPingCallback callback) {
        connector(PING_PROTOCOL).newClient().connect(address, new NetworkClientCallback<Object>() {
            @Override
            public void onConnect(NetworkClient<Object> client) {
                client.disconnect();

                callback.onResult(address, NetworkPingResult.SUCCESS);
            }

            @Override
            public void onDisconnect(NetworkClient<Object> client, Optional<Throwable> cause) {
                cause.ifPresent(err -> {
                    if (err instanceof NetworkTimeoutException) {
                        callback.onResult(address, NetworkPingResult.TIMEOUT);
                    } else {
                        callback.onResult(address, NetworkPingResult.FAILURE);
                    }
                });
            }

            @Override
            public void onMessage(NetworkMessage<Object> message, NetworkClient<Object> client) {
                throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
            }
        });
    }

    @Override
    public NetworkServiceJmx jmx() {
        return new NetworkServiceJmx() {
            @Override
            public int getConnectTimeout() {
                return connectTimeout;
            }

            @Override
            public int getHeartbeatInterval() {
                return heartbeatInterval;
            }

            @Override
            public int getHeartbeatLossThreshold() {
                return heartbeatLossThreshold;
            }

            @Override
            public int getNioThreads() {
                return nioThreadPoolSize;
            }

            @Override
            public NetworkTransportType getTransport() {
                return transport;
            }

            @Override
            public boolean isSsl() {
                return serverSsl != null;
            }

            @Override
            public boolean isTcpNoDelay() {
                return tcpNoDelay;
            }

            @Override
            public Integer getTcpReceiveBufferSize() {
                return soReceiveBufferSize;
            }

            @Override
            public Integer getTcpSendBufferSize() {
                return soSendBufferSize;
            }

            @Override
            public Boolean getTcpReuseAddress() {
                return soReuseAddress;
            }
        };
    }

    protected <T> NettyClientFactory<T> createClientFactory() {
        return new NettyClientFactory<>();
    }

    private InetAddress ipOnly(InetAddress address) throws HekateException {
        if (address == null) {
            return null;
        }

        try {
            return InetAddress.getByName(address.getHostAddress());
        } catch (UnknownHostException e) {
            throw new HekateException("Failed to resolve host address [address=" + address + ']', e);
        }
    }

    private <T> ConnectorRegistration<T> register(NetworkConnectorConfig<T> cfg) {
        assert cfg != null : "Connector configuration is null.";

        // Sanity checks.
        ConfigCheck check = ConfigCheck.get(NetworkConnectorConfig.class);

        String protocol = cfg.getProtocol();

        check.notEmpty(protocol, "protocol");
        check.validSysName(protocol, "protocol");
        check.unique(protocol, connectors.keySet(), "protocol");

        // Decide which event loop to use for this connector.
        int nioThreads = cfg.getNioThreads();

        boolean useCoreLoop;
        EventLoopGroup eventLoop;

        if (nioThreads > 0) {
            useCoreLoop = false;

            eventLoop = newEventLoop(nioThreads, "NioWorker-" + protocol);
        } else {
            useCoreLoop = true;

            eventLoop = coreLoop;
        }

        // Resolve codec.
        CodecFactory<T> codecFactory;

        if (cfg.getMessageCodec() == null) {
            codecFactory = defaultCodecFactory();
        } else {
            codecFactory = cfg.getMessageCodec();
        }

        // Prepare factory.
        NettyClientFactory<T> factory = createClientFactory();

        // Connector-specific properties.
        factory.setProtocol(protocol);
        factory.setCodecFactory(codecFactory);
        factory.setIdleTimeout(cfg.getIdleSocketTimeout());
        factory.setLoggerCategory(cfg.getLogCategory());

        // Common properties.
        factory.setConnectTimeout(connectTimeout);
        factory.setSoReceiveBufferSize(soReceiveBufferSize);
        factory.setSoSendBufferSize(soSendBufferSize);
        factory.setSoReuseAddress(soReuseAddress);
        factory.setTcpNoDelay(tcpNoDelay);
        factory.setSsl(clientSsl);

        // Event loop.
        factory.setEventLoop(eventLoop);

        // Metrics.
        factory.setMetrics(metrics.createClientFactory().createSink(protocol));

        // Prepare server handler (if configured).
        NettyServerHandlerConfig<T> handlerCfg = null;

        if (cfg.getServerHandler() != null) {
            NetworkServerHandler<T> handler = cfg.getServerHandler();

            handlerCfg = new NettyServerHandlerConfig<>();

            handlerCfg.setProtocol(protocol);
            handlerCfg.setCodecFactory(codecFactory);
            handlerCfg.setLoggerCategory(cfg.getLogCategory());
            handlerCfg.setHandler(handler);

            if (!useCoreLoop) {
                handlerCfg.setEventLoop(eventLoop);
            }
        }

        // Register connector.
        NetworkConnector<T> conn = new DefaultNetworkConnector<>(
            protocol,
            nioThreads,
            handlerCfg != null, // <-- Has server handler.
            factory
        );

        ConnectorRegistration<T> reg = new ConnectorRegistration<>(
            useCoreLoop ? null : eventLoop, // <-- Decide which event loop to use for this connector.
            conn,
            handlerCfg
        );

        connectors.put(protocol, reg);

        return reg;
    }

    private NetworkConnectorConfig<Object> pingConnector() {
        NetworkConnectorConfig<Object> ping = new NetworkConnectorConfig<>();

        SingletonCodecFactory<Object> codecFactory = new SingletonCodecFactory<>(new Codec<Object>() {
            @Override
            public Object decode(DataReader in) throws IOException {
                throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
            }

            @Override
            public void encode(Object message, DataWriter out) throws IOException {
                throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
            }

            @Override
            public boolean isStateful() {
                return false;
            }

            @Override
            public Class<Object> baseType() {
                return Object.class;
            }
        });

        ping.setProtocol(PING_PROTOCOL);
        ping.setMessageCodec(codecFactory);
        ping.setServerHandler((message, from) -> {
            throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
        });

        return ping;
    }

    private <T> CodecFactory<T> defaultCodecFactory() {
        return codec.codecFactory();
    }

    private EventLoopGroup newEventLoop(int size, String threadNamePrefix) {
        switch (transport) {
            case EPOLL: {
                return new EpollEventLoopGroup(size, new HekateThreadFactory(threadNamePrefix));
            }
            case NIO: {
                return new NioEventLoopGroup(size, new HekateThreadFactory(threadNamePrefix));
            }
            case AUTO: // <-- Fail since AUTO must be resolved in the constructor.
            default: {
                throw new IllegalArgumentException("Unexpected transport type: " + transport);
            }
        }
    }

    private void shutdown(EventLoopGroup group) {
        NettyUtils.shutdown(group).awaitUninterruptedly();
    }

    @Override
    public String toString() {
        return ToString.format(NetworkService.class, this);
    }
}
