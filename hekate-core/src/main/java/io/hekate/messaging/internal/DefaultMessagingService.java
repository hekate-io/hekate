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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.failover.FailoverPolicy;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.unicast.LoadBalancer;
import io.hekate.metrics.MetricsService;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.util.StateGuard;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.joining;

public class DefaultMessagingService implements MessagingService, DependentService, ConfigurableService, InitializingService,
    TerminatingService, NetworkConfigProvider {

    private static final Logger log = LoggerFactory.getLogger(DefaultMessagingService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String MESSAGING_THREAD_PREFIX = "Messaging";

    private final StateGuard guard = new StateGuard(MessagingService.class);

    private final Map<String, MessagingGateway<?>> gateways = new HashMap<>();

    private final List<MessagingChannelConfig<?>> channelsConfig = new LinkedList<>();

    private ScheduledExecutorService scheduler;

    private ClusterNodeId nodeId;

    private CodecService codecService;

    private NetworkService net;

    private ClusterService cluster;

    private MetricsService metrics;

    public DefaultMessagingService(MessagingServiceFactory factory) {
        assert factory != null : "Factory is null.";

        Utils.nullSafe(factory.getChannels()).forEach(channelsConfig::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.configureMessaging()).forEach(channelsConfig::add)
        );
    }

    @Override
    public void resolve(DependencyContext ctx) {
        net = ctx.require(NetworkService.class);
        cluster = ctx.require(ClusterService.class);
        codecService = ctx.require(CodecService.class);

        metrics = ctx.optional(MetricsService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Collect configurations from providers.
        Collection<MessagingConfigProvider> providers = ctx.findComponents(MessagingConfigProvider.class);

        Utils.nullSafe(providers).forEach(provider -> {
            Collection<MessagingChannelConfig<?>> regions = provider.configureMessaging();

            Utils.nullSafe(regions).forEach(channelsConfig::add);
        });

        // Validate configs.
        ConfigCheck check = ConfigCheck.get(MessagingChannelConfig.class);

        Set<String> uniqueNames = new HashSet<>();

        channelsConfig.forEach(cfg -> {
            check.notEmpty(cfg.getName(), "name");

            String name = cfg.getName().trim();

            check.unique(name, uniqueNames, "name");

            MessagingBackPressureConfig pressureCfg = cfg.getBackPressure();

            if (pressureCfg != null) {
                int outHi = pressureCfg.getOutHighWatermark();
                int outLo = pressureCfg.getOutLowWatermark();
                int inHi = pressureCfg.getInHighWatermark();
                int inLo = pressureCfg.getInLowWatermark();

                MessagingOverflowPolicy outOverflow = pressureCfg.getOutOverflow();

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
        channelsConfig.stream()
            .filter(cfg -> cfg.getReceiver() != null)
            .forEach(cfg ->
                ctx.addServiceProperty(ChannelNodeFilter.CHANNELS_PROPERTY, cfg.getName().trim())
            );
    }

    @Override
    public Collection<NetworkConnectorConfig<?>> configureNetwork() {
        List<NetworkConnectorConfig<?>> connectors = new ArrayList<>(channelsConfig.size());

        channelsConfig.forEach(channelCfg ->
            connectors.add(getConnectorConfig(channelCfg))
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

            nodeId = ctx.getNode().getId();

            channelsConfig.forEach(this::registerChannel);

            cluster.addListener(this::updateTopology, ClusterEventType.JOIN, ClusterEventType.CHANGE);

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
                if (scheduler != null) {
                    waiting.add(Utils.shutdown(scheduler));

                    scheduler = null;
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

            gateways.values().forEach(g -> channels.add(g.getChannel()));

            return channels;
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public <T> DefaultMessagingChannel<T> channel(String channelName) {
        ArgAssert.notNull(channelName, "Channel name");

        guard.lockReadWithStateCheck();

        try {
            @SuppressWarnings("unchecked")
            MessagingGateway<T> gateway = (MessagingGateway<T>)gateways.get(channelName);

            ArgAssert.check(gateway != null, "No such channel [name=" + channelName + ']');

            return gateway.getChannel();
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
        int nioThreads = cfg.getNioThreads();
        int workerThreads = cfg.getWorkerThreads();
        long idleTimeout = cfg.getIdleTimeout();
        long messagingTimeout = cfg.getMessagingTimeout();
        MessageReceiver<T> receiver = cfg.getReceiver();
        ClusterNodeFilter filter = cfg.getClusterFilter();
        FailoverPolicy failover = cfg.getFailoverPolicy();
        LoadBalancer<T> loadBalancer = cfg.getLoadBalancer();
        MessagingBackPressureConfig pressureCfg = cfg.getBackPressure();

        ClusterNode localNode = cluster.getNode();
        ClusterView clusterView = cluster.filter(new ChannelNodeFilter(name, filter));

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
        HekateThreadFactory asyncThreadFactory = new HekateThreadFactory(MESSAGING_THREAD_PREFIX + '-' + name);

        AffinityExecutor async;

        if (workerThreads > 0) {
            async = new AsyncAffinityExecutor(asyncThreadFactory, workerThreads);
        } else {
            async = new SyncAffinityExecutor(asyncThreadFactory);
        }

        // Prepare metrics.
        MetricsCallback channelMetrics = metrics != null ? new MetricsCallback(name, metrics) : null;

        // Prepare back pressure guards.
        SendBackPressure sendBackPressure = null;
        ReceiveBackPressure receiveBackPressure = null;

        if (pressureCfg != null) {
            int inHiWatermark = pressureCfg.getInHighWatermark();
            int inLoWatermark = pressureCfg.getInLowWatermark();
            int outHiWatermark = pressureCfg.getOutHighWatermark();
            int outLoWatermark = pressureCfg.getOutLowWatermark();
            MessagingOverflowPolicy outOverflow = pressureCfg.getOutOverflow();

            if (outOverflow != MessagingOverflowPolicy.IGNORE) {
                sendBackPressure = new SendBackPressure(outLoWatermark, outHiWatermark, outOverflow);
            }

            if (inHiWatermark > 0) {
                receiveBackPressure = new ReceiveBackPressure(inLoWatermark, inHiWatermark);
            }
        }

        // Create gateway.
        MessageReceiver<T> guardedReceiver = applyGuard(receiver);

        MessagingGateway<T> gateway = new MessagingGateway<>(name, connector, localNode, clusterView, guardedReceiver, nioThreads, async,
            channelMetrics, receiveBackPressure, sendBackPressure, failover, messagingTimeout, loadBalancer, checkIdle,
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
            });

        // Register gateway.
        gateways.put(name, gateway);

        // Schedule idle connections checking (if required).
        if (checkIdle) {
            // Lazy initialize scheduler.
            if (scheduler == null) {
                HekateThreadFactory threadFactory = new HekateThreadFactory(MESSAGING_THREAD_PREFIX);

                scheduler = Executors.newSingleThreadScheduledExecutor(threadFactory);
            }

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

            ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(idleCheckTask, idleTimeout, idleTimeout, TimeUnit.MILLISECONDS);

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

    private <T> NetworkConnectorConfig<MessagingProtocol> getConnectorConfig(MessagingChannelConfig<T> cfg) {
        assert cfg != null : "Channel configuration is null.";

        String name = cfg.getName().trim();
        MessageReceiver<T> receiver = cfg.getReceiver();
        String logCategory = cfg.getLogCategory();
        int socketThreadPoolSize = cfg.getNioThreads();

        CodecFactory<T> codecFactory = resolveCodecFactory(cfg);

        NetworkConnectorConfig<MessagingProtocol> netCfg = new NetworkConnectorConfig<>();

        netCfg.setProtocol(name);
        netCfg.setLogCategory(logCategory);
        netCfg.setMessageCodec(() -> new MessagingProtocolCodec<>(codecFactory.createCodec()));

        if (socketThreadPoolSize > 0) {
            netCfg.setNioThreads(socketThreadPoolSize);
        }

        if (receiver != null) {
            netCfg.setServerHandler(new NetworkServerHandler<MessagingProtocol>() {
                @Override
                public void onConnect(MessagingProtocol message, NetworkEndpoint<MessagingProtocol> client) {
                    MessagingProtocol.Connect connect = (MessagingProtocol.Connect)message;

                    // Reject connections if their target node doesn't match with the local node.
                    // This can happen in rare cases if node is restarted on the same address and remote nodes
                    // haven't detected cluster topology change yet.
                    if (!connect.getTo().equals(nodeId)) {
                        // Channel rejected connection.
                        client.disconnect();

                        return;
                    }

                    @SuppressWarnings("unchecked")
                    MessagingGateway<T> gateway = (MessagingGateway<T>)gateways.get(client.getProtocol());

                    // Reject connection to unknown channel.
                    if (gateway == null) {
                        client.disconnect();

                        return;
                    }

                    MessagingChannelId channelId = connect.getChannelId();

                    MessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(channelId, gateway.getChannel());

                    NetReceiverContext<T> ctx = new NetReceiverContext<>(client, endpoint, gateway);

                    // Try to register connection within the gateway.
                    if (gateway.registerServerReceiver(ctx)) {
                        client.setContext(ctx);

                        ctx.onConnect();
                    } else {
                        // Gateway rejected connection.
                        client.disconnect();
                    }
                }

                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> msg, NetworkEndpoint<MessagingProtocol> from) throws IOException {
                    NetReceiverContext<T> ctx = from.getContext();

                    if (ctx != null) {
                        ctx.receive(msg, from);
                    }
                }

                @Override
                public void onDisconnect(NetworkEndpoint<MessagingProtocol> client) {
                    NetReceiverContext<T> ctx = client.getContext();

                    if (ctx != null) {
                        ctx.onDisconnect();
                    }
                }
            });
        }

        return netCfg;
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

    private <T> CodecFactory<T> resolveCodecFactory(MessagingChannelConfig<T> cfg) {
        CodecFactory<T> codecFactory = getDefaultIfNull(cfg.getMessageCodec());

        if (codecFactory.createCodec().isStateful()) {
            return codecFactory;
        } else {
            return new ThreadLocalCodecFactory<>(codecFactory);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> CodecFactory<T> getDefaultIfNull(CodecFactory<T> factory) {
        if (factory == null) {
            return (CodecFactory<T>)codecService.getCodecFactory();
        }

        return factory;
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
