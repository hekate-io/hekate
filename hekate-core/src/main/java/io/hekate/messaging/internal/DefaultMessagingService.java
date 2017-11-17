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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.MessageInterceptor;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.loadbalance.LoadBalancer;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.joining;

public class DefaultMessagingService implements MessagingService, DependentService, ConfigurableService, InitializingService,
    TerminatingService, NetworkConfigProvider, ClusterAcceptor {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessagingService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String MESSAGING_THREAD_PREFIX = "Messaging";

    private final StateGuard guard = new StateGuard(MessagingService.class);

    private final Map<String, MessagingGateway<?>> gateways = new HashMap<>();

    private final List<MessagingChannelConfig<?>> channelsConfig = new LinkedList<>();

    private Hekate hekate;

    private ScheduledThreadPoolExecutor timer;

    private ClusterNodeId nodeId;

    private CodecService codecService;

    private NetworkService net;

    private ClusterService cluster;

    private LocalMetricsService metrics;

    public DefaultMessagingService(MessagingServiceFactory factory) {
        assert factory != null : "Factory is null.";

        StreamUtils.nullSafe(factory.getChannels()).forEach(channelsConfig::add);

        StreamUtils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            StreamUtils.nullSafe(provider.configureMessaging()).forEach(channelsConfig::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        hekate = ctx.hekate();

        net = ctx.require(NetworkService.class);
        cluster = ctx.require(ClusterService.class);
        codecService = ctx.require(CodecService.class);

        metrics = ctx.optional(LocalMetricsService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<MessagingConfigProvider> providers = ctx.findComponents(MessagingConfigProvider.class);

        StreamUtils.nullSafe(providers).forEach(provider -> {
            Collection<MessagingChannelConfig<?>> regions = provider.configureMessaging();

            StreamUtils.nullSafe(regions).forEach(channelsConfig::add);
        });

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(MessagingChannelConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        channelsConfig.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");
            check.notNull(cfg.getBaseType(), "base type");
            check.positive(cfg.getPartitions(), "partitions");
            check.isPowerOfTwo(cfg.getPartitions(), "partitions size");

            Class<?> codecType = codecFactory(cfg).createCodec().baseType();

            check.isTrue(codecType.isAssignableFrom(cfg.getBaseType()), "channel type must be a sub-class of message codec type "
                + "[channel-type=" + cfg.getBaseType().getName() + ", codec-type=" + codecType.getName() + ']');

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            MessagingBackPressureConfig pressureCfg = cfg.getBackPressure();

            if (pressureCfg != null) {
                int outHi = pressureCfg.getOutHighWatermark();
                int outLo = pressureCfg.getOutLowWatermark();
                int inHi = pressureCfg.getInHighWatermark();
                int inLo = pressureCfg.getInLowWatermark();

                MessagingOverflowPolicy outOverflow = pressureCfg.getOutOverflowPolicy();

                check.notNull(outOverflow, "outbound queue overflow policy");

                if (outOverflow != MessagingOverflowPolicy.IGNORE) {
                    check.positive(outHi, "outbound queue high watermark");

                    check.that(outHi > outLo, "outbound queue high watermark must be greater than low watermark.");
                }

                if (inHi > 0) {
                    check.that(inHi > inLo, "inbound queue high watermark must be greater than low watermark.");
                }
            }

            uniqueNames.add(name);
        });

        // Register channel names as a service property.
        channelsConfig.forEach(cfg -> {
            ChannelMetaData meta = new ChannelMetaData(
                cfg.hasReceiver(),
                cfg.getBaseType().getName(),
                cfg.getPartitions(),
                cfg.getBackupNodes()
            );

            ctx.setStringProperty(ChannelMetaData.propertyName(cfg.getName()), meta.toString());
        });
    }

    @Override
    public String acceptJoin(ClusterNode joining, Hekate local) {
        if (joining.hasService(MessagingService.class)) {
            ServiceInfo locService = local.localNode().service(MessagingService.class);
            ServiceInfo remService = joining.service(MessagingService.class);

            for (MessagingChannelConfig<?> cfg : channelsConfig) {
                String channelName = cfg.getName().trim();

                ChannelMetaData locMeta = ChannelMetaData.parse(locService.stringProperty(ChannelMetaData.propertyName(channelName)));
                ChannelMetaData remMeta = ChannelMetaData.parse(remService.stringProperty(ChannelMetaData.propertyName(channelName)));

                if (remMeta != null) {
                    if (!locMeta.type().equals(remMeta.type())) {
                        return "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                            + "'baseType' value mismatch between the joining node and the cluster "
                            + "[channel=" + cfg.getName()
                            + ", joining-type=" + remMeta.type()
                            + ", cluster-type=" + locMeta.type()
                            + ", rejected-by=" + local.localNode().address()
                            + ']';
                    }

                    if (locMeta.partitions() != remMeta.partitions()) {
                        return "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                            + "'partitions' value mismatch between the joining node and the cluster "
                            + "[channel=" + cfg.getName()
                            + ", joining-value=" + remMeta.partitions()
                            + ", cluster-value=" + locMeta.partitions()
                            + ", rejected-by=" + local.localNode().address()
                            + ']';
                    }

                    if (locMeta.backupNodes() != remMeta.backupNodes()) {
                        return "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                            + "'backupNodes' value mismatch between the joining node and the cluster "
                            + "[channel=" + cfg.getName()
                            + ", joining-value=" + remMeta.backupNodes()
                            + ", cluster-value=" + locMeta.backupNodes()
                            + ", rejected-by=" + local.localNode().address()
                            + ']';
                    }
                }
            }
        }

        return null;
    }

    @Override
    public Collection<NetworkConnectorConfig<?>> configureNetwork() {
        if (channelsConfig.isEmpty()) {
            return Collections.emptyList();
        }

        List<NetworkConnectorConfig<?>> connectors = new ArrayList<>(channelsConfig.size());

        channelsConfig.forEach(channelCfg ->
            connectors.add(toNetworkConfig(channelCfg))
        );

        return connectors;
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            if (!channelsConfig.isEmpty()) {
                nodeId = ctx.localNode().id();

                HekateThreadFactory timerFactory = new HekateThreadFactory(MESSAGING_THREAD_PREFIX + "Timer");

                timer = new ScheduledThreadPoolExecutor(1, timerFactory);

                timer.setRemoveOnCancelPolicy(true);

                channelsConfig.forEach(this::registerChannel);

                cluster.addListener(this::updateTopology, ClusterEventType.JOIN, ClusterEventType.CHANGE);
            }

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void preTerminate() throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeTerminating();
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() {
        List<Waiting> waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating...");
                }

                waiting = new ArrayList<>();

                // Close all gateways.
                // Need to create a copy since gateways can remove themselves from the gateways map.
                ArrayList<MessagingGateway<?>> gatewaysCopy = new ArrayList<>(gateways.values());

                waiting.addAll(gatewaysCopy.stream()
                    .map(MessagingGateway::close)
                    .collect(Collectors.toList())
                );

                gateways.clear();

                // Shutdown scheduler.
                if (timer != null) {
                    waiting.add(AsyncUtils.shutdown(timer));

                    timer = null;
                }

                nodeId = null;
            }
        } finally {
            guard.unlockWrite();
        }

        if (waiting != null) {
            Waiting.awaitAll(waiting).awaitUninterruptedly();

            if (DEBUG) {
                log.debug("Terminated.");
            }
        }
    }

    @Override
    public List<MessagingChannel<?>> allChannels() {
        guard.lockReadWithStateCheck();

        try {
            List<MessagingChannel<?>> channels = new ArrayList<>(gateways.size());

            gateways.values().forEach(g -> channels.add(g.channel()));

            return channels;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public DefaultMessagingChannel<Object> channel(String name) throws IllegalArgumentException {
        return channel(name, null);
    }

    @Override
    public <T> DefaultMessagingChannel<T> channel(String name, Class<T> baseType) throws IllegalArgumentException {
        ArgAssert.notNull(name, "Channel name");

        guard.lockReadWithStateCheck();

        try {
            @SuppressWarnings("unchecked")
            MessagingGateway<T> gateway = (MessagingGateway<T>)gateways.get(name);

            ArgAssert.check(gateway != null, "No such channel [name=" + name + ']');

            if (baseType != null && !gateway.baseType().isAssignableFrom(baseType)) {
                throw new ClassCastException("Messaging channel doesn't support the specified type "
                    + "[channel-type=" + gateway.baseType().getName() + ", requested-type=" + baseType.getName() + ']');
            }

            return gateway.channel();
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public boolean hasChannel(String channelName) {
        guard.lockReadWithStateCheck();

        try {
            return gateways.containsKey(channelName);
        } finally {
            guard.unlockRead();
        }
    }

    private <T> MessagingGateway<T> registerChannel(MessagingChannelConfig<T> cfg) {
        assert cfg != null : "Channel configuration is null.";
        assert guard.isWriteLocked() : "Thread must hold a write lock.";

        if (DEBUG) {
            log.debug("Creating a new channel [config={}]", cfg);
        }

        String name = cfg.getName().trim();
        Class<T> baseType = cfg.getBaseType();
        int nioThreads = cfg.getNioThreads();
        int workerThreads = cfg.getWorkerThreads();
        long idleTimeout = cfg.getIdleSocketTimeout();
        long messagingTimeout = cfg.getMessagingTimeout();
        int partitions = cfg.getPartitions();
        int backupNodes = cfg.getBackupNodes();
        MessageReceiver<T> receiver = cfg.getReceiver();
        ClusterNodeFilter filter = cfg.getClusterFilter();
        FailoverPolicy failover = cfg.getFailoverPolicy();
        LoadBalancer<T> loadBalancer = cfg.getLoadBalancer();
        MessageInterceptor<T> interceptor = cfg.getInterceptor();
        MessagingBackPressureConfig pressureCfg = cfg.getBackPressure();

        ClusterNode localNode = cluster.localNode();
        ClusterView clusterView = cluster.filter(ChannelMetaData.hasReceiver(name, filter));

        NetworkConnector<MessagingProtocol> connector = net.connector(name);

        // Check if channel has idle connections timeout.
        boolean checkIdle;

        AtomicReference<Future<?>> idleCheckTaskRef;

        if (idleTimeout > 0) {
            checkIdle = true;

            idleCheckTaskRef = new AtomicReference<>();
        } else {
            checkIdle = false;

            idleCheckTaskRef = null;
        }

        // Prepare thread pool for asynchronous messages processing.
        HekateThreadFactory asyncFactory = new HekateThreadFactory(MESSAGING_THREAD_PREFIX + '-' + name);

        MessagingExecutor async;

        if (workerThreads > 0) {
            async = new MessagingExecutorAsync(asyncFactory, workerThreads, timer);
        } else {
            async = new MessagingExecutorSync(asyncFactory, timer);
        }

        // Prepare metrics.
        MetricsCallback channelMetrics = metrics != null ? new MetricsCallback(name, metrics) : null;

        // Prepare back pressure guards.
        SendPressureGuard sendPressureGuard = null;
        ReceivePressureGuard receivePressureGuard = null;

        if (pressureCfg != null) {
            int inHiWatermark = pressureCfg.getInHighWatermark();
            int inLoWatermark = pressureCfg.getInLowWatermark();
            int outHiWatermark = pressureCfg.getOutHighWatermark();
            int outLoWatermark = pressureCfg.getOutLowWatermark();
            MessagingOverflowPolicy outOverflow = pressureCfg.getOutOverflowPolicy();

            if (outOverflow != MessagingOverflowPolicy.IGNORE) {
                sendPressureGuard = new SendPressureGuard(outLoWatermark, outHiWatermark, outOverflow);
            }

            if (inHiWatermark > 0) {
                receivePressureGuard = new ReceivePressureGuard(inLoWatermark, inHiWatermark);
            }
        }

        // Create gateway.
        MessageReceiver<T> guardedReceiver = applyGuard(receiver);

        MessagingGateway<T> gateway = new MessagingGateway<>(
            name,
            hekate,
            baseType,
            connector,
            localNode,
            clusterView,
            guardedReceiver,
            nioThreads,
            async,
            channelMetrics,
            receivePressureGuard,
            sendPressureGuard,
            failover,
            messagingTimeout,
            loadBalancer,
            interceptor,
            partitions,
            backupNodes,
            logger(cfg),
            checkIdle,
            // Before close callback.
            () -> {
                if (DEBUG) {
                    log.debug("Closing channel [name={}]", name);
                }

                // Cancel idle connections checking.
                if (checkIdle) {
                    Future<?> idleCheckTask = idleCheckTaskRef.getAndSet(null);

                    if (idleCheckTask != null) {
                        if (DEBUG) {
                            log.debug("Canceling idle channel handling task [channel={}]", name);
                        }

                        idleCheckTask.cancel(false);
                    }
                }

                gateways.remove(name);
            }
        );

        // Register gateway.
        gateways.put(name, gateway);

        // Schedule idle connections checking (if required).
        if (checkIdle) {
            if (DEBUG) {
                log.debug("Scheduling new task for idle channel handling [check-interval={}]", idleTimeout);
            }

            Runnable idleCheckTask = () -> {
                try {
                    gateway.checkIdleConnections();
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected error while checking for idle connections [channel={}]", name, e);
                }
            };

            ScheduledFuture<?> future = timer.scheduleWithFixedDelay(idleCheckTask, idleTimeout, idleTimeout, TimeUnit.MILLISECONDS);

            idleCheckTaskRef.set(future);
        }

        return gateway;
    }

    private <T> MessageReceiver<T> applyGuard(final MessageReceiver<T> receiver) {
        if (receiver == null) {
            return null;
        } else {
            // Decorate receiver with service state checks.
            return new GuardedMessageReceiver<>(guard, receiver);
        }
    }

    private <T> NetworkConnectorConfig<MessagingProtocol> toNetworkConfig(MessagingChannelConfig<T> cfg) {
        assert cfg != null : "Channel configuration is null.";

        NetworkConnectorConfig<MessagingProtocol> net = new NetworkConnectorConfig<>();

        net.setProtocol(cfg.getName().trim());
        net.setLogCategory(loggerName(cfg));

        CodecFactory<T> codecFactory = safeCodecFactory(cfg);

        net.setMessageCodec(() ->
            new MessagingProtocolCodec<>(codecFactory.createCodec())
        );

        if (cfg.getNioThreads() > 0) {
            net.setNioThreads(cfg.getNioThreads());
        }

        if (cfg.getReceiver() != null) {
            net.setServerHandler(new NetworkServerHandler<MessagingProtocol>() {
                @Override
                public void onConnect(MessagingProtocol message, NetworkEndpoint<MessagingProtocol> client) {
                    MessagingProtocol.Connect connect = (MessagingProtocol.Connect)message;

                    // Reject connections if their target node doesn't match with the local node.
                    // This can happen in rare cases if node is restarted on the same address and remote nodes
                    // haven't detected cluster topology change yet.
                    if (!connect.to().equals(nodeId)) {
                        // Channel rejected connection.
                        client.disconnect();

                        return;
                    }

                    @SuppressWarnings("unchecked")
                    MessagingGateway<T> gateway = (MessagingGateway<T>)gateways.get(client.protocol());

                    // Reject connection to unknown channel.
                    if (gateway == null) {
                        client.disconnect();

                        return;
                    }

                    ClusterNodeId from = connect.from();

                    MessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(from, gateway.channel());

                    MessagingConnectionNetIn<T> conn = new MessagingConnectionNetIn<>(client, endpoint, gateway);

                    // Try to register connection within the gateway.
                    if (gateway.register(conn)) {
                        client.setContext(conn);

                        conn.onConnect();
                    } else {
                        // Gateway rejected connection.
                        client.disconnect();
                    }
                }

                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> msg, NetworkEndpoint<MessagingProtocol> from) throws IOException {
                    MessagingConnectionNetIn<T> conn = from.getContext();

                    if (conn != null) {
                        conn.receive(msg, from);
                    }
                }

                @Override
                public void onDisconnect(NetworkEndpoint<MessagingProtocol> client) {
                    MessagingConnectionNetIn<T> ctx = client.getContext();

                    if (ctx != null) {
                        ctx.onDisconnect();
                    }
                }
            });
        }

        return net;
    }

    private void updateTopology(ClusterEvent event) {
        assert event != null : "Topology is null.";

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                gateways.values().forEach(MessagingGateway::updateTopology);
            }
        } finally {
            guard.unlockRead();
        }
    }

    private <T> CodecFactory<T> safeCodecFactory(MessagingChannelConfig<T> cfg) {
        CodecFactory<T> factory = codecFactory(cfg);

        if (factory.createCodec().isStateful()) {
            return factory;
        } else {
            return new ThreadLocalCodecFactory<>(factory);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> CodecFactory<T> codecFactory(MessagingChannelConfig<T> cfg) {
        if (cfg.getMessageCodec() == null) {
            return (CodecFactory<T>)codecService.codecFactory();
        }

        return cfg.getMessageCodec();
    }

    private <T> Logger logger(MessagingChannelConfig<T> cfg) {
        return LoggerFactory.getLogger(loggerName(cfg));
    }

    private String loggerName(MessagingChannelConfig<?> cfg) {
        String name = Utils.nullOrTrim(cfg.getLogCategory());

        return name != null ? name : MessagingGateway.class.getName();
    }

    @Override
    public String toString() {
        String serverChannels = channelsConfig.stream()
            .filter(c -> c.getReceiver() != null)
            .map(MessagingChannelConfig::getName)
            .collect(joining(", ", "{", "}"));

        String clientChannels = channelsConfig.stream()
            .filter(c -> c.getReceiver() == null)
            .map(MessagingChannelConfig::getName)
            .collect(joining(", ", "{", "}"));

        return MessagingService.class.getSimpleName()
            + "[client-channels=" + clientChannels
            + ", server-channels=" + serverChannels
            + ']';
    }
}
