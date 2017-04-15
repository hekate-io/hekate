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
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.codec.SingletonCodecFactory;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.MetricsService;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.NetworkTransportType;
import io.hekate.network.PingCallback;
import io.hekate.network.PingResult;
import io.hekate.network.address.AddressSelector;
import io.hekate.network.internal.NetworkBindCallback;
import io.hekate.network.internal.NetworkServer;
import io.hekate.network.internal.NetworkServerCallback;
import io.hekate.network.internal.NetworkServerFailure;
import io.hekate.network.internal.NetworkServerFuture;
import io.hekate.network.internal.NetworkServiceManager;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.netty.channel.ConnectTimeoutException;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NettyNetworkService implements NetworkServiceManager, DependentService, ConfigurableService, InitializingService,
    TerminatingService {
    private static class ConnectorRegistration<T> {
        private final String protocol;

        private final EventLoopGroup eventLoopGroup;

        private final NettyServerHandlerConfig<T> serverHandler;

        private final NetworkConnector<T> connector;

        public ConnectorRegistration(String protocol, EventLoopGroup eventLoopGroup, NetworkConnector<T> connector,
            NettyServerHandlerConfig<T> serverHandler) {
            assert protocol != null : "Protocol is null.";
            assert connector != null : "Connector is null.";

            this.protocol = protocol;
            this.eventLoopGroup = eventLoopGroup;
            this.serverHandler = serverHandler;
            this.connector = connector;
        }

        public String getProtocol() {
            return protocol;
        }

        public boolean hasEventLoopGroup() {
            return eventLoopGroup != null;
        }

        public EventLoopGroup getEventLoopGroup() {
            return eventLoopGroup;
        }

        public NettyServerHandlerConfig<T> getServerHandler() {
            return serverHandler;
        }

        public NetworkConnector<T> getConnector() {
            return connector;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(NettyNetworkService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final int NIO_ACCEPTOR_THREADS = 1;

    private static final String PING_PROTOCOL = "hekate.ping";

    private final String initHost;

    private final AddressSelector addressSelector;

    private final int initPort;

    private final int portRange;

    private final int connectTimeout;

    private final long acceptorFailoverInterval;

    private final int heartbeatInterval;

    private final int heartbeatLossThreshold;

    private final int nioThreadPoolSize;

    private final NetworkTransportType transport;

    private final boolean tcpNoDelay;

    private final Integer soReceiveBufferSize;

    private final Integer soSendBufferSize;

    private final Boolean soReuseAddress;

    private final Integer soBacklog;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(NetworkService.class);

    @ToStringIgnore
    private final List<NetworkConnectorConfig<?>> connectorConfigs = new ArrayList<>();

    @ToStringIgnore
    private final Map<String, ConnectorRegistration<?>> connectors = new HashMap<>();

    @ToStringIgnore
    private CodecService codecService;

    @ToStringIgnore
    private NettyMetricsAdaptor metrics;

    @ToStringIgnore
    private EventLoopGroup acceptorGroup;

    @ToStringIgnore
    private EventLoopGroup coreWorkerGroup;

    @ToStringIgnore
    private NettyServer server;

    public NettyNetworkService(NetworkServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(NetworkServiceFactory.class);

        check.range(factory.getPort(), 0, 65535, "port");
        check.notNull(factory.getTransport(), "transport");
        check.notNull(factory.getAddressSelector(), "address selector");
        check.positive(factory.getNioThreads(), "NIO thread pool size");
        check.positive(factory.getHeartbeatInterval(), "heartbeat interval");
        check.positive(factory.getHeartbeatLossThreshold(), "heartbeat loss threshold");
        check.positive(factory.getConnectTimeout(), "connect timeout");

        initHost = factory.getHost();
        initPort = factory.getPort();
        portRange = factory.getPortRange();
        addressSelector = factory.getAddressSelector();
        connectTimeout = factory.getConnectTimeout();
        acceptorFailoverInterval = factory.getAcceptRetryInterval();
        heartbeatInterval = factory.getHeartbeatInterval();
        heartbeatLossThreshold = factory.getHeartbeatLossThreshold();
        nioThreadPoolSize = factory.getNioThreads();
        tcpNoDelay = factory.isTcpNoDelay();
        soReceiveBufferSize = factory.getTcpReceiveBufferSize();
        soSendBufferSize = factory.getTcpSendBufferSize();
        soReuseAddress = factory.getTcpReuseAddress();
        soBacklog = factory.getTcpBacklog();

        if (factory.getTransport() == NetworkTransportType.AUTO) {
            transport = Epoll.isAvailable() ? NetworkTransportType.EPOLL : NetworkTransportType.NIO;
        } else {
            transport = factory.getTransport();
        }

        Utils.nullSafe(factory.getConnectors()).forEach(connectorConfigs::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.configureNetwork()).forEach(connectorConfigs::add)
        );

        // Register ping protocol.
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
        });

        ping.setProtocol(PING_PROTOCOL);
        ping.setMessageCodec(codecFactory);
        ping.setServerHandler((message, from) -> {
            throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
        });

        connectorConfigs.add(ping);
    }

    @Override
    public void resolve(DependencyContext ctx) {
        codecService = ctx.require(CodecService.class);

        MetricsService metricsService = ctx.optional(MetricsService.class);

        if (metricsService != null) {
            metrics = createMetricsAdaptor(metricsService);
        }
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        Collection<NetworkConfigProvider> providers = ctx.findComponents(NetworkConfigProvider.class);

        providers.forEach(provider ->
            Utils.nullSafe(provider.configureNetwork()).forEach(connectorConfigs::add)
        );
    }

    @Override
    public NetworkServerFuture bind(NetworkBindCallback callback) throws HekateException {
        ArgAssert.notNull(callback, "Callback");

        if (DEBUG) {
            log.debug("Obtaining preferred host address...");
        }

        InetAddress preferredIp = getPreferredIp();

        if (DEBUG) {
            log.debug("Obtained preferred host address [address={}]", preferredIp);
        }

        InetAddress publicIp = ipOnly(addressSelector.select(preferredIp));

        if (publicIp == null) {
            throw new HekateException("Failed to select public host address [selector=" + addressSelector + ']');
        }

        if (DEBUG) {
            log.debug("Selected public host address [address={}]", publicIp);
        }

        InetSocketAddress bindAddress = new InetSocketAddress(preferredIp, initPort);

        log.info("Binding network service [preferred-address={}]", bindAddress);

        guard.lockWrite();

        try {
            guard.becomeInitialized();

            acceptorGroup = newEventLoopGroup(NIO_ACCEPTOR_THREADS, "NioAcceptor");
            coreWorkerGroup = newEventLoopGroup(nioThreadPoolSize, "NioWorker-core");

            NettyServerFactory serverFactory = new NettyServerFactory();

            serverFactory.setAutoAccept(false);
            serverFactory.setHeartbeatInterval(heartbeatInterval);
            serverFactory.setHeartbeatLossThreshold(heartbeatLossThreshold);
            serverFactory.setSoBacklog(soBacklog);
            serverFactory.setSoReceiveBufferSize(soReceiveBufferSize);
            serverFactory.setSoSendBufferSize(soSendBufferSize);
            serverFactory.setSoReuseAddress(soReuseAddress);
            serverFactory.setTcpNoDelay(tcpNoDelay);
            serverFactory.setAcceptorEventLoopGroup(acceptorGroup);
            serverFactory.setWorkerEventLoopGroup(coreWorkerGroup);

            server = serverFactory.createServer();

            connectorConfigs.forEach(protocolCfg -> {
                ConnectorRegistration<?> reg = createRegistration(protocolCfg);

                connectors.put(reg.getProtocol(), reg);

                if (reg.getServerHandler() != null) {
                    server.addHandler(reg.getServerHandler());
                }
            });

            return server.start(bindAddress, new NetworkServerCallback() {
                @Override
                public void onStart(NetworkServer server) {
                    InetSocketAddress realAddress = server.getAddress();

                    if (log.isInfoEnabled()) {
                        log.info("Done binding network acceptor [bind-address={}]", realAddress);
                    }

                    // Convert to public address (port can be assigned by the auto-increment or by OS if it was configured as 0).
                    InetSocketAddress address = new InetSocketAddress(publicIp, realAddress.getPort());

                    callback.onBind(address);
                }

                @Override
                public NetworkServerFailure.Resolution onFailure(NetworkServer server, NetworkServerFailure err) {
                    Throwable cause = err.getCause();

                    if (cause instanceof IOException) {
                        int initPort = bindAddress.getPort();

                        if (initPort > 0 && portRange > 0 && server.getState() == NetworkServer.State.STARTING) {
                            int prevPort = err.getLastTriedAddress().getPort();

                            int newPort = prevPort + 1;

                            if (newPort < initPort + portRange) {
                                InetSocketAddress newAddress = new InetSocketAddress(err.getLastTriedAddress().getAddress(), newPort);

                                if (log.isInfoEnabled()) {
                                    log.info("Couldn't bind on port {} ...will try next port [new-address={}]", prevPort, newAddress);
                                }

                                return err.retry().withRetryAddress(newAddress);
                            }
                        } else if (server.getState() == NetworkServer.State.STARTED && acceptorFailoverInterval > 0) {
                            if (log.isErrorEnabled()) {
                                log.error("Network server encountered an error ...will try to restart after {} ms [attempt={}, address={}]",
                                    acceptorFailoverInterval, err.getAttempt(), err.getLastTriedAddress(), cause);
                            }

                            return err.retry().withRetryDelay(acceptorFailoverInterval);
                        }
                    }

                    return callback.onFailure(err);
                }
            });
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void initialize(InitializationContext ctx) {
        // No-op.
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
                if (metrics != null) {
                    server.setMetrics(metrics);
                }

                log.info("Started accepting network connections.");

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
        EventLoopGroup localAcceptorGroup = null;
        EventLoopGroup localCoreGroup = null;
        List<EventLoopGroup> localGroups = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                localServer = this.server;
                localAcceptorGroup = this.acceptorGroup;
                localCoreGroup = this.coreWorkerGroup;

                localGroups = connectors.values().stream()
                    .filter(ConnectorRegistration::hasEventLoopGroup)
                    .map(ConnectorRegistration::getEventLoopGroup)
                    .collect(Collectors.toList());

                connectors.clear();

                acceptorGroup = null;
                coreWorkerGroup = null;
                server = null;
                metrics = null;
            }
        } finally {
            guard.unlockWrite();
        }

        if (localServer != null) {
            try {
                Utils.getUninterruptedly(localServer.stop());
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

        shutdown(localAcceptorGroup);
        shutdown(localCoreGroup);

        if (localGroups != null) {
            localGroups.forEach(this::shutdown);
        }
    }

    @Override
    public <T> NetworkConnector<T> connector(String protocol) throws IllegalArgumentException {
        ArgAssert.notNull(protocol, "Protocol");

        guard.tryLockReadWithStateCheck();

        try {
            ConnectorRegistration<?> module = connectors.get(protocol);

            ArgAssert.check(module != null, "Unknown protocol [name=" + protocol + ']');

            @SuppressWarnings("unchecked")
            NetworkConnector<T> connector = (NetworkConnector<T>)module.getConnector();

            return connector;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public boolean hasConnector(String protocol) {
        guard.tryLockReadWithStateCheck();

        try {
            return connectors.containsKey(protocol);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void ping(InetSocketAddress address, PingCallback callback) {
        connector(PING_PROTOCOL).newClient().connect(address, new NetworkClientCallback<Object>() {
            @Override
            public void onConnect(NetworkClient<Object> client) {
                client.disconnect();

                callback.onResult(address, PingResult.SUCCESS);
            }

            @Override
            public void onDisconnect(NetworkClient<Object> client, Optional<Throwable> cause) {
                cause.ifPresent(err -> {
                    if (err instanceof ConnectTimeoutException || err instanceof SocketTimeoutException) {
                        callback.onResult(address, PingResult.TIMEOUT);
                    } else {
                        callback.onResult(address, PingResult.FAILURE);
                    }
                });
            }

            @Override
            public void onMessage(NetworkMessage<Object> message, NetworkClient<Object> client) {
                throw new UnsupportedOperationException(PING_PROTOCOL + " doesn't support any messages.");
            }
        });
    }

    // Package level for testing purposes.
    void start() {
        // Safe to use null since we don't use this parameter in any of those method.
        preInitialize(null);
        initialize(null);
        postInitialize(null);
    }

    // Package level for testing purposes.
    void stop() {
        preTerminate();
        terminate();
        postTerminate();
    }

    private InetAddress getPreferredIp() throws HekateException {
        if (initHost == null || initHost.isEmpty()) {
            return new InetSocketAddress(0).getAddress();
        }

        try {
            InetAddress host = InetAddress.getByName(initHost.trim());

            return ipOnly(host);
        } catch (UnknownHostException e) {
            throw new HekateException("Failed to resolve host address [address=" + initHost + ']', e);
        }
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

    private <T> ConnectorRegistration<T> createRegistration(NetworkConnectorConfig<T> cfg) {
        ConfigCheck check = ConfigCheck.get(NetworkConnectorConfig.class);

        String protocol = cfg.getProtocol();

        check.notEmpty(protocol, "protocol");
        check.unique(protocol, connectors.keySet(), "protocol");

        boolean useCoreGroup;
        EventLoopGroup eventLoopGroup;

        if (cfg.getNioThreads() > 0) {
            useCoreGroup = false;

            eventLoopGroup = newEventLoopGroup(cfg.getNioThreads(), "NioWorker-" + protocol);
        } else {
            useCoreGroup = true;
            eventLoopGroup = coreWorkerGroup;
        }

        CodecFactory<T> codecFactory;

        if (cfg.getMessageCodec() == null) {
            codecFactory = getDefaultCodecFactory();
        } else {
            codecFactory = cfg.getMessageCodec();
        }

        NettyClientFactory<T> factory = new NettyClientFactory<>();

        factory.setProtocol(protocol);
        factory.setCodecFactory(codecFactory);
        factory.setIdleTimeout(cfg.getIdleTimeout());
        factory.setLoggerCategory(cfg.getLogCategory());

        factory.setConnectTimeout(connectTimeout);
        factory.setSoReceiveBufferSize(soReceiveBufferSize);
        factory.setSoSendBufferSize(soSendBufferSize);
        factory.setSoReuseAddress(soReuseAddress);
        factory.setTcpNoDelay(tcpNoDelay);

        factory.setEventLoopGroup(eventLoopGroup);

        if (metrics != null) {
            NettyMetricsCallback metricsCallback = metrics.createCallback(false, protocol);

            factory.setMetrics(metricsCallback);
        }

        NettyServerHandlerConfig<T> handlerCfg = null;
        Optional<NetworkServerHandler<T>> optHandler;

        if (cfg.getServerHandler() != null) {
            NetworkServerHandler<T> handler = cfg.getServerHandler();

            handlerCfg = new NettyServerHandlerConfig<>();

            handlerCfg.setProtocol(protocol);
            handlerCfg.setCodecFactory(codecFactory);
            handlerCfg.setLoggerCategory(cfg.getLogCategory());
            handlerCfg.setHandler(handler);

            if (!useCoreGroup) {
                handlerCfg.setEventLoopGroup(eventLoopGroup);
            }

            optHandler = Optional.of(handler);
        } else {
            optHandler = Optional.empty();
        }

        NetworkConnector<T> connector = new DefaultNetworkConnector<>(protocol, factory, optHandler);

        return new ConnectorRegistration<>(protocol, !useCoreGroup ? eventLoopGroup : null, connector, handlerCfg);
    }

    private NettyMetricsAdaptor createMetricsAdaptor(MetricsService metrics) {
        // Overall bytes metrics.
        CounterMetric allBytesSent = counter(NettyMetricsAdaptor.BYTES_OUT, true, metrics);
        CounterMetric allBytesReceived = counter(NettyMetricsAdaptor.BYTES_IN, true, metrics);

        // Overall message metrics.
        CounterMetric allMsgSent = counter(NettyMetricsAdaptor.MSG_OUT, true, metrics);
        CounterMetric allMsgReceived = counter(NettyMetricsAdaptor.MSG_IN, true, metrics);
        CounterMetric allMsgQueue = counter(NettyMetricsAdaptor.MSG_QUEUE, false, metrics);
        CounterMetric allMsgFailed = counter(NettyMetricsAdaptor.MSG_ERR, true, metrics);

        // Overall connection metrics.
        CounterMetric allConnections = counter(NettyMetricsAdaptor.CONN_ACTIVE, false, metrics);

        return (server, protocol) -> {
            // Connector bytes metrics.
            CounterMetric bytesSent = counter(NettyMetricsAdaptor.BYTES_OUT, protocol, server, true, metrics);
            CounterMetric bytesReceived = counter(NettyMetricsAdaptor.BYTES_IN, protocol, server, true, metrics);

            // Connector message metrics.
            CounterMetric msgSent = counter(NettyMetricsAdaptor.MSG_OUT, protocol, server, true, metrics);
            CounterMetric msgReceived = counter(NettyMetricsAdaptor.MSG_IN, protocol, server, true, metrics);
            CounterMetric msgQueue = counter(NettyMetricsAdaptor.MSG_QUEUE, protocol, server, false, metrics);
            CounterMetric msgFailed = counter(NettyMetricsAdaptor.MSG_ERR, protocol, server, true, metrics);

            // Connector connection metrics.
            CounterMetric connections = counter(NettyMetricsAdaptor.CONN_ACTIVE, protocol, server, false, metrics);

            return new NettyMetricsCallback() {
                @Override
                public void onBytesSent(long bytes) {
                    bytesSent.add(bytes);

                    allBytesSent.add(bytes);
                }

                @Override
                public void onBytesReceived(long bytes) {
                    bytesReceived.add(bytes);

                    allBytesReceived.add(bytes);
                }

                @Override
                public void onMessageSent() {
                    msgSent.increment();

                    allMsgSent.increment();
                }

                @Override
                public void onMessageReceived() {
                    msgReceived.increment();

                    allMsgReceived.increment();
                }

                @Override
                public void onMessageSendError() {
                    msgFailed.increment();

                    allMsgFailed.increment();
                }

                @Override
                public void onMessageEnqueue() {
                    msgQueue.increment();

                    allMsgQueue.increment();
                }

                @Override
                public void onMessageDequeue() {
                    msgQueue.decrement();

                    allMsgQueue.decrement();
                }

                @Override
                public void onConnect() {
                    connections.increment();

                    allConnections.increment();
                }

                @Override
                public void onDisconnect() {
                    connections.decrement();

                    allConnections.decrement();
                }
            };
        };
    }

    private <T> CodecFactory<T> getDefaultCodecFactory() {
        return codecService.getCodecFactory();
    }

    private EventLoopGroup newEventLoopGroup(int size, String threadNamePrefix) {
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
        if (group != null) {
            group.shutdownGracefully(0, Long.MAX_VALUE, TimeUnit.NANOSECONDS).awaitUninterruptibly();
        }
    }

    private static CounterMetric counter(String name, boolean autoReset, MetricsService metrics) {
        return counter(name, null, null, autoReset, metrics);
    }

    private static CounterMetric counter(String name, String protocol, Boolean server, boolean autoReset, MetricsService metrics) {
        String counterName = "";

        if (protocol != null) {
            counterName += protocol + '.';
        }

        counterName += "network.";

        if (server != null) {
            counterName += server ? "server." : "client.";
        }

        counterName += name;

        CounterConfig cfg = new CounterConfig(counterName);

        cfg.setAutoReset(autoReset);

        if (autoReset) {
            cfg.setName(counterName + ".current");
            cfg.setTotalName(counterName + ".total");
        } else {
            cfg.setName(counterName);
        }

        return metrics.register(cfg);
    }

    @Override
    public String toString() {
        return ToString.format(NetworkService.class, this);
    }
}
