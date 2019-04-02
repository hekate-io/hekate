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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
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
import io.hekate.core.jmx.JmxService;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.messaging.MessageReceiver;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.messaging.MessagingService;
import io.hekate.messaging.MessagingServiceFactory;
import io.hekate.messaging.intercept.MessageInterceptor;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.ExtendedScheduledExecutor;
import io.hekate.util.async.Waiting;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class DefaultMessagingService implements MessagingService, DependentService, ConfigurableService, InitializingService,
    TerminatingService, NetworkConfigProvider, ClusterAcceptor {
    private static final Logger log = LoggerFactory.getLogger(DefaultMessagingService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String MESSAGING_THREAD_PREFIX = "Messaging";

    private final StateGuard guard = new StateGuard(MessagingService.class);

    private final MessagingServiceFactory factory;

    private final Map<String, MessagingGateway<?>> gateways = new HashMap<>();

    private ExtendedScheduledExecutor timer;

    private CodecService codec;

    private NetworkService net;

    private ClusterService cluster;

    private JmxService jmx;

    // Volatile since accessed out of the guarded context.
    private volatile ClusterNodeId nodeId;

    public DefaultMessagingService(MessagingServiceFactory factory) {
        assert factory != null : "Factory is null.";

        this.factory = factory;
    }

    @Override
    public void resolve(DependencyContext ctx) {
        net = ctx.require(NetworkService.class);
        cluster = ctx.require(ClusterService.class);
        codec = ctx.require(CodecService.class);

        jmx = ctx.optional(JmxService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        List<MessageInterceptor> interceptors = StreamUtils.nullSafe(factory.getGlobalInterceptors()).collect(toList());

        // Collect channel configurations.
        StreamUtils.nullSafe(factory.getChannels()).forEach(cfg ->
            register(cfg, interceptors)
        );

        StreamUtils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            StreamUtils.nullSafe(provider.configureMessaging()).forEach(cfg ->
                register(cfg, interceptors)
            )
        );

        Collection<MessagingConfigProvider> providers = ctx.findComponents(MessagingConfigProvider.class);

        StreamUtils.nullSafe(providers).forEach(provider -> {
            Collection<MessagingChannelConfig<?>> regions = provider.configureMessaging();

            StreamUtils.nullSafe(regions).forEach(cfg ->
                register(cfg, interceptors)
            );
        });

        // Register channel meta-data as a service property.
        gateways.values().forEach(proxy -> {
            MessagingMetaData meta = new MessagingMetaData(
                proxy.hasReceiver(),
                proxy.baseType().getName()
            );

            ctx.setStringProperty(MessagingMetaData.propertyName(proxy.name()), meta.toString());
        });
    }

    @Override
    public String acceptJoin(ClusterNode joining, Hekate local) {
        if (joining.hasService(MessagingService.class)) {
            ServiceInfo locService = local.localNode().service(MessagingService.class);
            ServiceInfo remService = joining.service(MessagingService.class);

            for (MessagingGateway<?> gateway : gateways.values()) {
                String channel = gateway.name();

                MessagingMetaData locMeta = MessagingMetaData.parse(locService.stringProperty(MessagingMetaData.propertyName(channel)));
                MessagingMetaData remMeta = MessagingMetaData.parse(remService.stringProperty(MessagingMetaData.propertyName(channel)));

                if (remMeta != null) {
                    if (!locMeta.type().equals(remMeta.type())) {
                        return "Invalid " + MessagingChannelConfig.class.getSimpleName() + " - "
                            + "'baseType' value mismatch between the joining node and the cluster "
                            + "[channel=" + channel
                            + ", joining-type=" + remMeta.type()
                            + ", cluster-type=" + locMeta.type()
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
        if (gateways.isEmpty()) {
            return Collections.emptyList();
        }

        List<NetworkConnectorConfig<?>> connectors = new ArrayList<>(gateways.size());

        gateways.values().forEach(proxy ->
            connectors.add(networkConfigFor(proxy))
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

            if (!gateways.isEmpty()) {
                nodeId = ctx.localNode().id();

                timer = newTimer();

                for (MessagingGateway<?> gateway : gateways.values()) {
                    initializeGateway(gateway, ctx.metrics());
                }

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

                // Close all gateways.
                waiting = gateways.values().stream()
                    .map(MessagingGateway::context)
                    .filter(Objects::nonNull)
                    .map(MessagingGatewayContext::close)
                    .collect(toList());

                // Shutdown timer.
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

            gateways.values().forEach(proxy -> channels.add(proxy.context().channel()));

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

            return gateway.context().channel();
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public boolean hasChannel(String channelName) {
        return gateways.containsKey(channelName);
    }

    private <T> void register(MessagingChannelConfig<T> cfg, List<MessageInterceptor> interceptors) {
        ConfigCheck check = ConfigCheck.get(MessagingChannelConfig.class);

        // Validate configuration.
        check.notEmpty(cfg.getName(), "name");
        check.validSysName(cfg.getName(), "name");
        check.notNull(cfg.getBaseType(), "base type");
        check.positive(cfg.getPartitions(), "partitions");
        check.isPowerOfTwo(cfg.getPartitions(), "partitions size");

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

        MessagingGateway<T> gateway = new MessagingGateway<>(cfg, cluster, codec, interceptors);

        // Check uniqueness of the channel name.
        check.unique(gateway.name(), gateways.keySet(), "name");

        // Check that the channel's base type is supported by the codec.
        Class<?> codecType = gateway.codecFactory().createCodec().baseType();

        check.isTrue(codecType.isAssignableFrom(cfg.getBaseType()), "channel type must be a sub-class of message codec type "
            + "[channel-type=" + cfg.getBaseType().getName() + ", codec-type=" + codecType.getName() + ']');

        gateways.put(gateway.name(), gateway);
    }

    private <T> void initializeGateway(MessagingGateway<T> gateway, MeterRegistry metrics) throws HekateException {
        assert gateway != null : "Channel gateway is null.";
        assert guard.isWriteLocked() : "Thread must hold a write lock.";

        if (DEBUG) {
            log.debug("Creating a new messaging gateway [context={}]", gateway);
        }

        // Prepare network connector.
        NetworkConnector<MessagingProtocol> connector = net.connector(gateway.name());

        // Prepare thread pool for asynchronous messages processing.
        MessagingExecutor async;

        if (gateway.workerThreads() > 0) {
            async = new MessagingExecutorAsync(gateway.workerThreads(), newThreadFactory(gateway.name()));
        } else {
            async = new MessagingExecutorSync(newThreadFactory(gateway.name()));
        }

        // Prepare metrics.
        MessagingMetrics channelMetrics = new MessagingMetrics(gateway.name(), async::activeTasks, async::completedTasks, metrics);

        // Make sure that receiver is guarded with lock.
        MessageReceiver<T> guardedReceiver = applyGuard(gateway.unguardedReceiver());

        // Create context.
        MessagingGatewayContext<T> ctx = new MessagingGatewayContext<>(
            gateway.name(),
            gateway.baseType(),
            connector,
            cluster.localNode(),
            guardedReceiver,
            async,
            timer,
            channelMetrics,
            gateway.receivePressureGuard(),
            gateway.sendPressureGuard(),
            gateway.interceptors(),
            gateway.log(),
            gateway.idleSocketTimeout() > 0, /* <-- Check for idle connections.*/
            gateway.messagingTimeout(),
            gateway.warnOnRetry(),
            gateway.baseRetryPolicy(),
            gateway.rootChannel()
        );

        // Schedule idle connections checking (if required).
        long idleTimeout = gateway.idleSocketTimeout();

        if (idleTimeout > 0) {
            if (DEBUG) {
                log.debug("Scheduling new task for idle channel handling [check-interval={}]", idleTimeout);
            }

            timer.repeatWithFixedDelay(() -> {
                try {
                    ctx.checkIdleConnections();
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected error while checking for idle connections [channel={}]", gateway.name(), e);
                }

                return !ctx.isClosed();
            }, idleTimeout, idleTimeout, TimeUnit.MILLISECONDS);
        }

        // Initialize the gateway with a new context.
        gateway.init(ctx);

        // Register to JMX (optional).
        if (jmx != null) {
            jmx.register(new DefaultMessagingChannelJmx(gateway), ctx.name());
        }
    }

    private <T> MessageReceiver<T> applyGuard(final MessageReceiver<T> receiver) {
        if (receiver == null) {
            return null;
        } else {
            // Decorate receiver with service state checks.
            return new GuardedMessageReceiver<>(guard, receiver);
        }
    }

    private <T> NetworkConnectorConfig<MessagingProtocol> networkConfigFor(MessagingGateway<T> gateway) {
        assert gateway != null : "Channel gateway is null.";

        NetworkConnectorConfig<MessagingProtocol> net = new NetworkConnectorConfig<>();

        net.setProtocol(gateway.name());
        net.setLogCategory(gateway.logCategory());

        CodecFactory<T> codecFactory = gateway.codecFactory();

        net.setMessageCodec(() ->
            new MessagingProtocolCodec<>(codecFactory.createCodec())
        );

        if (gateway.nioThreads() > 0) {
            net.setNioThreads(gateway.nioThreads());
        }

        if (gateway.hasReceiver()) {
            net.setServerHandler(new NetworkServerHandler<MessagingProtocol>() {
                @Override
                public void onConnect(MessagingProtocol message, NetworkEndpoint<MessagingProtocol> client) {
                    MessagingProtocol.Connect connect = (MessagingProtocol.Connect)message;

                    // Reject connections if their target node doesn't match with the local node.
                    // This can happen in rare cases if node is restarted on the same port and remote nodes
                    // haven't detected the cluster topology change yet.
                    if (!connect.to().equals(nodeId)) {
                        // Channel rejected connection.
                        client.disconnect();

                        return;
                    }

                    @SuppressWarnings("unchecked")
                    MessagingGateway<T> connectTo = (MessagingGateway<T>)gateways.get(client.protocol());

                    // Reject connection to unknown channel.
                    if (connectTo == null) {
                        client.disconnect();

                        return;
                    }

                    // Reject connection if channel is not initialized.
                    MessagingGatewayContext<T> ctx = connectTo.context();

                    if (ctx == null) {
                        client.disconnect();

                        return;
                    }

                    ClusterAddress from = connect.from();

                    MessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(from, ctx.channel());

                    MessagingConnectionIn<T> conn = new MessagingConnectionIn<>(client, endpoint, ctx);

                    // Try to register connection within the gateway.
                    if (ctx.register(conn)) {
                        client.setContext(conn);

                        conn.onConnect();
                    } else {
                        // Gateway rejected connection.
                        client.disconnect();
                    }
                }

                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> msg, NetworkEndpoint<MessagingProtocol> from) throws IOException {
                    MessagingConnectionIn<?> conn = (MessagingConnectionIn<?>)from.getContext();

                    if (conn != null) {
                        conn.receive(msg, from);
                    }
                }

                @Override
                public void onDisconnect(NetworkEndpoint<MessagingProtocol> client) {
                    MessagingConnectionIn<?> clientCtx = (MessagingConnectionIn<?>)client.getContext();

                    if (clientCtx != null) {
                        clientCtx.onDisconnect();
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
                gateways.values().forEach(proxy ->
                    proxy.context().checkTopologyChanges()
                );
            }
        } finally {
            guard.unlockRead();
        }
    }

    private static ExtendedScheduledExecutor newTimer() {
        ExtendedScheduledExecutor timer = new ExtendedScheduledExecutor(1, newThreadFactory("Timer"));

        timer.setRemoveOnCancelPolicy(true);

        return timer;
    }

    private static HekateThreadFactory newThreadFactory(String suffix) {
        return new HekateThreadFactory(MESSAGING_THREAD_PREFIX + '-' + suffix);
    }

    @Override
    public String toString() {
        String serverChannels = gateways.values().stream()
            .filter(MessagingGateway::hasReceiver)
            .map(MessagingGateway::name)
            .collect(joining(", ", "{", "}"));

        String clientChannels = gateways.values().stream()
            .filter(proxy -> !proxy.hasReceiver())
            .map(MessagingGateway::name)
            .collect(joining(", ", "{", "}"));

        return MessagingService.class.getSimpleName()
            + "[client-channels=" + clientChannels
            + ", server-channels=" + serverChannels
            + ']';
    }
}
