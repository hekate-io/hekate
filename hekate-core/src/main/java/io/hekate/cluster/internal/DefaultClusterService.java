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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterJoinRejectedException;
import io.hekate.cluster.ClusterJoinValidator;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.internal.gossip.GossipCommManager;
import io.hekate.cluster.internal.gossip.GossipListener;
import io.hekate.cluster.internal.gossip.GossipManager;
import io.hekate.cluster.internal.gossip.GossipNodeStatus;
import io.hekate.cluster.internal.gossip.GossipPolicy;
import io.hekate.cluster.internal.gossip.GossipProtocol;
import io.hekate.cluster.internal.gossip.GossipProtocol.GossipMessage;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.HeartbeatRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinAccept;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReject;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinReply;
import io.hekate.cluster.internal.gossip.GossipProtocol.JoinRequest;
import io.hekate.cluster.internal.gossip.GossipProtocol.UpdateBase;
import io.hekate.cluster.internal.gossip.GossipProtocolCodec;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateConfigurationException;
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
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.NetworkConfigProvider;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkService;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toSet;

public class DefaultClusterService implements ClusterService, DependentService, ConfigurableService, InitializingService,
    TerminatingService, NetworkConfigProvider {
    private static class DeferredListener {
        private final ClusterEventListener listener;

        private final ClusterEventType[] eventTypes;

        public DeferredListener(ClusterEventListener listener, ClusterEventType[] eventTypes) {
            this.listener = listener;
            this.eventTypes = eventTypes;
        }

        public ClusterEventListener getListener() {
            return listener;
        }

        public ClusterEventType[] getEventTypes() {
            return eventTypes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof DeferredListener)) {
                return false;
            }

            DeferredListener that = (DeferredListener)o;

            return listener.equals(that.listener);
        }

        @Override
        public int hashCode() {
            return listener.hashCode();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final ClusterJoinValidator DEFAULT_JOIN_VALIDATOR = (newNode, local) -> {
        boolean localLoopback = local.getLocalNode().getSocket().getAddress().isLoopbackAddress();

        boolean remoteLoopback = newNode.getSocket().getAddress().isLoopbackAddress();

        String rejectReason = null;

        if (localLoopback != remoteLoopback) {
            if (localLoopback) {
                rejectReason = "Cluster is configured with loopback addresses while node is configured to use a non-loopback address.";
            } else {
                rejectReason = "Cluster is configured with non-loopback addresses while node is configured to use a loopback address.";
            }
        }

        return rejectReason;
    };

    private final long gossipInterval;

    private final int speedUpGossipSize;

    private final SeedNodeProvider seedNodeProvider;

    private final FailureDetector failureDetector;

    private final SplitBrainDetector splitBrainDetector;

    private final SplitBrainAction splitBrainAction;

    @ToStringIgnore
    private final AtomicBoolean splitBrainDetectorActive = new AtomicBoolean();

    @ToStringIgnore
    private final List<ClusterJoinValidator> joinValidators;

    @ToStringIgnore
    private final Set<ClusterNodeId> asyncJoinValidations = Collections.synchronizedSet(new HashSet<>());

    @ToStringIgnore
    private final GossipListener gossipSpy;

    @ToStringIgnore
    private final StateGuard guard;

    @ToStringIgnore
    private final AtomicReference<ClusterNodeId> localNodeIdRef = new AtomicReference<>();

    @ToStringIgnore
    private final List<ClusterEventListener> initListeners;

    @ToStringIgnore
    private final List<DeferredListener> deferredListeners = new CopyOnWriteArrayList<>();

    @ToStringIgnore
    private SeedNodeManager seedNodeMgr;

    @ToStringIgnore
    private GossipManager gossipMgr;

    @ToStringIgnore
    private NetworkService net;

    @ToStringIgnore
    private LocalMetricsService metrics;

    @ToStringIgnore
    private ClusterMetricsCallback metricsCallback;

    @ToStringIgnore
    private ScheduledExecutorService serviceThread;

    @ToStringIgnore
    private ScheduledExecutorService gossipThread;

    @ToStringIgnore
    private ScheduledFuture<?> heartbeatTask;

    @ToStringIgnore
    private ScheduledFuture<?> gossipTask;

    @ToStringIgnore
    private ScheduledFuture<?> joinTask;

    @ToStringIgnore
    private volatile InitializationContext ctx;

    @ToStringIgnore
    private volatile GossipCommManager commMgr;

    @ToStringIgnore
    private volatile ClusterNode node;

    public DefaultClusterService(ClusterServiceFactory factory, StateGuard guard, GossipListener gossipSpy) {
        ConfigCheck check = ConfigCheck.get(ClusterServiceFactory.class);

        check.notNull(factory, "configuration");
        check.positive(factory.getGossipInterval(), "gossip interval");
        check.notNull(factory.getFailureDetector(), "failure detector");
        check.notNull(factory.getSplitBrainAction(), "split-brain action");

        // Basic properties.
        this.gossipInterval = factory.getGossipInterval();
        this.speedUpGossipSize = factory.getSpeedUpGossipSize();
        this.failureDetector = factory.getFailureDetector();
        this.splitBrainAction = factory.getSplitBrainAction();
        this.splitBrainDetector = factory.getSplitBrainDetector();
        this.gossipSpy = gossipSpy;

        // Service lock.
        if (guard == null) {
            this.guard = new StateGuard(ClusterService.class);
        } else {
            this.guard = guard;
        }

        // Seed node provider.
        if (factory.getSeedNodeProvider() == null) {
            try {
                seedNodeProvider = new MulticastSeedNodeProvider();
            } catch (UnknownHostException e) {
                throw new HekateConfigurationException(HekateBootstrap.class.getSimpleName() + ": multicasting is not supported. "
                    + "Consider using other seed node provider implementation.", e);
            }
        } else {
            seedNodeProvider = factory.getSeedNodeProvider();
        }

        // Pre-configured event listeners.
        List<ClusterEventListener> initListeners = new ArrayList<>();

        initListeners.add(new ClusterEventLogger());

        Utils.nullSafe(factory.getClusterListeners()).forEach(initListeners::add);

        this.initListeners = unmodifiableList(initListeners);

        // Join validators.
        joinValidators = new ArrayList<>();

        joinValidators.add(DEFAULT_JOIN_VALIDATOR);

        Utils.nullSafe(factory.getJoinValidators()).forEach(joinValidators::add);
    }

    @Override
    public void resolve(DependencyContext ctx) {
        net = ctx.require(NetworkService.class);

        metrics = ctx.optional(LocalMetricsService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        Collection<ClusterJoinValidator> customAcceptors = ctx.findComponents(ClusterJoinValidator.class);

        joinValidators.addAll(customAcceptors);
    }

    @Override
    public Collection<NetworkConnectorConfig<?>> configureNetwork() {
        NetworkConnectorConfig<GossipProtocol> netCfg = new NetworkConnectorConfig<>();

        netCfg.setProtocol(GossipProtocolCodec.PROTOCOL_ID);
        netCfg.setMessageCodec(() -> new GossipProtocolCodec(localNodeIdRef));
        netCfg.setLogCategory(GossipCommManager.class.getName());

        // Use a dedicated NIO thread for cluster communications.
        netCfg.setNioThreads(1);

        netCfg.setServerHandler(new NetworkServerHandler<GossipProtocol>() {
            @Override
            public void onConnect(GossipProtocol login, NetworkEndpoint<GossipProtocol> client) {
                GossipCommManager localCommMgr = commMgr;

                if (localCommMgr != null) {
                    localCommMgr.onConnect(login, client);
                }
            }

            @Override
            public void onMessage(NetworkMessage<GossipProtocol> msg, NetworkEndpoint<GossipProtocol> from) throws IOException {
                GossipCommManager localCommMgr = commMgr;

                if (localCommMgr != null) {
                    localCommMgr.onMessage(msg, from);
                }
            }

            @Override
            public void onDisconnect(NetworkEndpoint<GossipProtocol> client) {
                GossipCommManager localCommMgr = commMgr;

                if (localCommMgr != null) {
                    localCommMgr.onDisconnect(client);
                }
            }
        });

        return Collections.singleton(netCfg);
    }

    @Override
    public void initialize(InitializationContext initCtx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            this.ctx = initCtx;

            node = initCtx.getNode();

            seedNodeMgr = new SeedNodeManager(initCtx.getClusterName(), seedNodeProvider);

            localNodeIdRef.set(node.getId());

            // Prepare workers.
            gossipThread = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("ClusterGossip"));
            serviceThread = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("Cluster"));

            // Register listeners from service configuration.
            initListeners.forEach(listener -> ctx.getCluster().addListener(listener));

            // Register deferred listeners.
            deferredListeners.forEach(deferred -> ctx.getCluster().addListener(deferred.getListener(), deferred.getEventTypes()));

            // Prepare gossip manager.
            gossipMgr = new GossipManager(initCtx.getClusterName(), node, failureDetector, speedUpGossipSize, createGossipListener());

            // Prepare gossip communication manager.
            NetworkConnector<GossipProtocol> connector = net.connector(GossipProtocolCodec.PROTOCOL_ID);

            commMgr = new GossipCommManager(connector, new GossipCommManager.Callback() {
                @Override
                public void onReceive(GossipProtocol msg) {
                    process(msg);
                }

                @Override
                public void onSendSuccess(GossipProtocol msg) {
                    // No-op.
                }

                @Override
                public void onSendFailure(GossipProtocol msg, Throwable error) {
                    processSendFailure(msg, error);
                }
            });

            // Prepare metrics callback (optional).
            if (metrics != null) {
                metricsCallback = new ClusterMetricsCallback(metrics);
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void postInitialize(InitializationContext ctx) throws HekateException {
        initJoining(ctx);
    }

    @Override
    public void preTerminate() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (ctx.getState() == Hekate.State.LEAVING) {
                    runOnGossipThread(this::doLeave);
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void terminate() throws HekateException {
        List<Waiting> waiting = new ArrayList<>();

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (seedNodeMgr != null) {
                    waiting.add(seedNodeMgr.stopCleaning());

                    if (node != null) {
                        InetSocketAddress address = node.getSocket();

                        SeedNodeManager localSeedNodeManager = this.seedNodeMgr;

                        waiting.add(() -> localSeedNodeManager.stopDiscovery(address));
                    }
                }

                if (gossipThread != null) {
                    waiting.add(Utils.shutdown(gossipThread));
                }

                if (serviceThread != null) {
                    waiting.add(Utils.shutdown(serviceThread));
                }

                if (commMgr != null) {
                    GossipCommManager localCommMgr = commMgr;

                    waiting.add(localCommMgr::stop);
                }

                if (failureDetector != null) {
                    waiting.add(failureDetector::terminate);
                }

                asyncJoinValidations.clear();

                localNodeIdRef.set(null);

                node = null;
                commMgr = null;
                gossipMgr = null;
                serviceThread = null;
                gossipThread = null;
                seedNodeMgr = null;
                metricsCallback = null;
            }
        } finally {
            guard.unlockWrite();
        }

        Waiting.awaitAll(waiting).awaitUninterruptedly();
    }

    @Override
    public ClusterTopology getTopology() {
        InitializationContext localCtx = getRequiredContext();

        return localCtx.getCluster().getTopology();
    }

    @Override
    public ClusterView filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        return new FilteredClusterView(this, filter);
    }

    @Override
    public void addListener(ClusterEventListener listener) {
        addListener(listener, (ClusterEventType[])null);
    }

    @Override
    public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
        ArgAssert.notNull(listener, "Listener");

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                getRequiredContext().getCluster().addListener(listener);
            } else {
                deferredListeners.add(new DeferredListener(listener, eventTypes));
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                getRequiredContext().getCluster().removeListener(listener);
            } else {
                deferredListeners.remove(new DeferredListener(listener, null));
            }
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        ArgAssert.notNull(predicate, "Predicate");

        CompletableFuture<ClusterTopology> future = new CompletableFuture<>();

        ClusterEventListener listener = new ClusterEventListener() {
            @Override
            public void onEvent(ClusterEvent event) {
                InitializationContext localCtx = getRequiredContext();

                if (future.isDone()) {
                    localCtx.getCluster().removeListener(this);
                } else if (predicate.test(event.getTopology())) {
                    localCtx.getCluster().removeListener(this);

                    future.complete(event.getTopology());
                } else if (event.getType() == ClusterEventType.LEAVE) {
                    localCtx.getCluster().removeListener(this);

                    future.cancel(false);
                }
            }
        };

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                getRequiredContext().getCluster().addListenerAsync(listener);
            } else {
                deferredListeners.add(new DeferredListener(listener, null));
            }
        } finally {
            guard.unlockRead();
        }

        return future;
    }

    @Override
    public ClusterNode getLocalNode() {
        ClusterNode node = this.node;

        if (node == null) {
            throw new IllegalStateException(ClusterService.class.getSimpleName() + " is not initialized.");
        }

        return node;
    }

    public SeedNodeProvider getSeedNodeProvider() {
        return seedNodeProvider;
    }

    public FailureDetector getFailureDetector() {
        return failureDetector;
    }

    public SplitBrainDetector getSplitBrainDetector() {
        return splitBrainDetector;
    }

    public SplitBrainAction getSplitBrainAction() {
        return splitBrainAction;
    }

    public List<ClusterJoinValidator> getJoinValidators() {
        return unmodifiableList(joinValidators);
    }

    private void initJoining(InitializationContext ctx) {
        // Prepare a repeatable join task to re-run in case of a recoverable failure during the join process.
        Runnable repeatableJoinTask = new Runnable() {
            @Override
            public void run() {
                ClusterAddress address;
                SeedNodeManager localSeedNodeMgr;

                guard.lockRead();

                try {
                    // Check that there were no concurrent leave/terminate events.
                    if (guard.isInitialized()) {
                        address = node.getAddress();
                        localSeedNodeMgr = seedNodeMgr;
                    } else {
                        return;
                    }
                } finally {
                    guard.unlockRead();
                }

                try {
                    // Start components.
                    // Note that we are doing it in unlocked context in order to prevent thread blocking.

                    // Check if node is not in a split-brain mode before trying to join.
                    if (splitBrainDetector != null) {
                        boolean valid = splitBrainDetector.isValid(node);

                        if (!valid) {
                            // Try to schedule a new join attempt.
                            guard.lockRead();

                            try {
                                // Check that there were no concurrent leave/terminate events.
                                if (guard.isInitialized()) {
                                    log.warn("Split-brain detected ...will wait for {} ms before making another attempt "
                                        + "[split-brain-detector={}]", gossipInterval, splitBrainDetector);

                                    serviceThread.schedule(this, gossipInterval, TimeUnit.MILLISECONDS);
                                }
                            } finally {
                                guard.unlockRead();
                            }

                            return;
                        }
                    }

                    // Start seed nodes discovery.
                    try {
                        localSeedNodeMgr.startDiscovery(address.getSocket());
                    } catch (HekateException e) {
                        boolean initialized = false;

                        // Try to schedule a new join attempt.
                        guard.lockRead();

                        try {
                            // Check that there were no concurrent leave/terminate events.
                            if (guard.isInitialized()) {
                                initialized = true;

                                log.error("Failed to start seed nodes discovery ...will wait for {}ms before making another attempt.",
                                    gossipInterval, e);

                                serviceThread.schedule(this, gossipInterval, TimeUnit.MILLISECONDS);
                            }
                        } finally {
                            guard.unlockRead();
                        }

                        if (!initialized) {
                            // Make sure that seed nodes discovery is stopped in case of concurrent service termination.
                            localSeedNodeMgr.stopDiscovery(address.getSocket());
                        }

                        return;
                    }

                    if (DEBUG) {
                        log.debug("Initializing failure detector [address={}]", address);
                    }

                    failureDetector.initialize(() -> address);

                    if (DEBUG) {
                        log.debug("Initialized failure detector [address={}]", address);
                    }

                    startJoining(address, localSeedNodeMgr, ctx);
                } catch (HekateException | RuntimeException | Error e) {
                    ctx.terminate(e);
                }
            }
        };

        runOnServiceThread(repeatableJoinTask);
    }

    private void startJoining(ClusterAddress address, SeedNodeManager localSeedNodeMgr, InitializationContext ctx) {
        ctx.getCluster().onStartJoining().thenAcceptAsync(proceed -> {
            boolean discard = false;

            guard.lockWrite();

            try {
                if (proceed && guard.isInitialized()) {
                    if (DEBUG) {
                        log.debug("Scheduling a periodic gossip task [interval={}]", gossipInterval);
                    }

                    // Schedule gossip task.
                    gossipTask = scheduleOn(gossipThread, this::gossip, gossipInterval);

                    // Schedule heartbeat task.
                    long hbInterval = failureDetector.getHeartbeatInterval();

                    if (hbInterval > 0) {
                        if (DEBUG) {
                            log.debug("Scheduling a periodic heartbeat task [interval={}]", hbInterval);
                        }

                        heartbeatTask = scheduleOn(gossipThread, this::heartbeat, hbInterval);
                    } else {
                        if (DEBUG) {
                            log.debug("Will not register a periodic heartbeat task [interval={}]", hbInterval);
                        }
                    }

                    if (DEBUG) {
                        log.debug("Scheduling an asynchronous join task [interval={}]", gossipInterval);
                    }

                    // Schedule task for asynchronous join.
                    joinTask = scheduleOn(serviceThread, this::doJoin, 0, gossipInterval);
                } else {
                    discard = true;
                }
            } catch (RuntimeException | Error e) {
                ctx.terminate(e);
            } finally {
                guard.unlockWrite();
            }

            // Operation was rejected.
            // Stop components (in unlocked context in order to prevent thread blocking).
            if (discard) {
                if (DEBUG) {
                    log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                }

                // Make sure that seed nodes discovery is stopped.
                localSeedNodeMgr.stopDiscovery(address.getSocket());

                // Make sure that failure detector is terminated.
                try {
                    failureDetector.terminate();
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected runtime error during the failure detector termination.", e);
                }
            }
        }, serviceThread);
    }

    private void doJoin() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                try {
                    List<InetSocketAddress> nodes = seedNodeMgr.getSeedNodes();

                    // Schedule join task to run on a gossiper thread.
                    runOnGossipThread(() -> {
                        guard.lockRead();

                        try {
                            if (guard.isInitialized()) {
                                JoinRequest msg = gossipMgr.join(nodes);

                                if (msg != null && log.isInfoEnabled()) {
                                    log.info("Sending cluster join request [seed-node={}].", msg.getToAddress());
                                }

                                sendAndClose(msg);
                            }
                        } catch (RuntimeException | Error t) {
                            log.error("Got runtime error while joining cluster.", t);
                        } finally {
                            guard.unlockRead();
                        }
                    });
                } catch (HekateException e) {
                    log.error("Failed to obtain seed nodes ...will wait for {} ms before trying another attempt.", gossipInterval, e);
                }
            }
        } catch (RuntimeException | Error t) {
            log.error("Got runtime error while joining cluster.", t);
        } finally {
            guard.unlockRead();
        }
    }

    private void doLeave() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (log.isInfoEnabled()) {
                    log.info("Leaving cluster...");
                }

                UpdateBase msg = gossipMgr.leave();

                send(msg);
            }
        } catch (RuntimeException | Error t) {
            log.error("Got runtime error while leaving cluster.", t);
        } finally {
            guard.unlockRead();
        }
    }

    private void gossip() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                gossipMgr.batchGossip(GossipPolicy.RANDOM_PREFER_UNSEEN).forEach(this::send);
            }
        } catch (RuntimeException | Error t) {
            log.error("Got runtime error while processing a gossip tick.", t);
        } finally {
            guard.unlockRead();
        }
    }

    private void heartbeat() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                // Check nodes aliveness.
                boolean failureDetected = gossipMgr.checkAliveness();

                // Send heartbeats first (even if new failures were detected).
                Collection<ClusterAddress> targets = failureDetector.heartbeatTick();

                if (targets != null) {
                    targets.stream().map(to -> new HeartbeatRequest(node.getAddress(), to)).forEach(this::send);
                }

                // Send gossip messages if new failures were detected.
                if (failureDetected) {
                    gossip();
                }
            }
        } catch (RuntimeException | Error t) {
            log.error("Got runtime error while processing heartbeats tick.", t);
        } finally {
            guard.unlockRead();
        }
    }

    private void process(GossipProtocol msg) {
        assert msg != null : "Message is null.";

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (metricsCallback != null) {
                    metricsCallback.onGossipMessage(msg.getType());
                }

                if (msg instanceof GossipMessage) {
                    GossipMessage gossipMsg = (GossipMessage)msg;

                    if (!node.getAddress().equals(gossipMsg.getTo())) {
                        if (DEBUG) {
                            log.debug("Ignored message since it is not addressed to the local node [message={}, node={}]", msg, node);
                        }

                        return;
                    }
                }

                GossipProtocol.Type type = msg.getType();

                if (type == GossipProtocol.Type.HEARTBEAT_REQUEST) {
                    boolean reply = failureDetector.onHeartbeatRequest(msg.getFrom());

                    if (reply) {
                        send(new HeartbeatReply(node.getAddress(), msg.getFrom()));
                    }
                } else if (type == GossipProtocol.Type.HEARTBEAT_REPLY) {
                    failureDetector.onHeartbeatReply(msg.getFrom());
                } else {
                    runOnGossipThread(() -> doProcess(msg));
                }
            } else {
                if (DEBUG) {
                    log.debug("Ignored message since service is not started [message={}]", msg);
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private void doProcess(GossipProtocol msg) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                switch (msg.getType()) {
                    case GOSSIP_UPDATE:
                    case GOSSIP_UPDATE_DIGEST: {
                        UpdateBase update = (UpdateBase)msg;

                        GossipMessage reply = gossipMgr.processUpdate(update);

                        send(reply);

                        break;
                    }
                    case JOIN_REQUEST: {
                        JoinRequest request = (JoinRequest)msg;

                        // Check that join request can be accepted.
                        JoinReject reject = gossipMgr.acceptJoinRequest(request);

                        if (reject == null) {
                            // Asynchronously validate and process join request so that validation would not block gossiping thread.
                            validateAndProcessAsync(request);
                        } else {
                            // Send immediate reject.
                            send(reject);
                        }

                        break;
                    }
                    case JOIN_ACCEPT: {
                        JoinAccept accept = (JoinAccept)msg;

                        GossipMessage reply = gossipMgr.processJoinAccept(accept);

                        send(reply);

                        break;
                    }
                    case JOIN_REJECT: {
                        JoinReject reject = (JoinReject)msg;

                        // Try to select another node to join.
                        JoinRequest newRequest = gossipMgr.processJoinReject(reject);

                        sendAndClose(newRequest);

                        break;
                    }
                    case HEARTBEAT_REQUEST:
                    case HEARTBEAT_REPLY:
                    case CONNECT:
                    default: {
                        throw new IllegalArgumentException("Unexpected message type: " + msg);
                    }
                }
            } else {
                if (DEBUG) {
                    log.debug("Ignored message since service is not started [message={}]", msg);
                }
            }
        } catch (RuntimeException | Error e) {
            log.error("Got runtime error while processing gossip message [message={}]", msg, e);
        } finally {
            guard.unlockRead();
        }
    }

    private void processSendFailure(GossipProtocol msg, Throwable error) {
        assert msg != null : "Message is null.";
        assert error != null : "Error is null.";

        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (msg.getType() == GossipProtocol.Type.JOIN_REQUEST) {
                    JoinRequest request = (JoinRequest)msg;

                    runOnGossipThread(() -> processJoinSendFailure(request));
                } else {
                    if (log.isDebugEnabled()) {
                        log.debug("Failed to sent gossip message [error={}, message={}]", error.toString(), msg);
                    }
                }
            } else {
                if (DEBUG) {
                    log.debug("Ignored message failure since service is not started [message={}]", msg);
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private void processJoinSendFailure(JoinRequest msg) {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                if (msg.getType() == GossipProtocol.Type.JOIN_REQUEST) {
                    if (DEBUG) {
                        log.debug("Processing join message send failure notification [message={}]", msg);
                    }

                    JoinRequest newReq = gossipMgr.processJoinFailure(msg);

                    sendAndClose(newReq);
                }
            } else {
                if (DEBUG) {
                    log.debug("Ignored message failure since service is not started [message={}]", msg);
                }
            }
        } catch (RuntimeException | Error e) {
            log.error("Got runtime error while processing notification on message submission failure [message={}]", msg, e);
        } finally {
            guard.unlockRead();
        }
    }

    private GossipListener createGossipListener() {
        return new GossipListener() {
            // Volatile since can be accessed by a different thread in seed node cleaner.
            private volatile Set<InetSocketAddress> knownAddresses = Collections.emptySet();

            @Override
            public void onJoinReject(ClusterAddress rejectedBy, String reason) {
                if (ctx.getState() == Hekate.State.JOINING) {
                    ctx.terminate(new ClusterJoinRejectedException(reason, rejectedBy));
                }
            }

            @Override
            public void onStatusChange(GossipNodeStatus oldStatus, GossipNodeStatus newStatus, int order, Set<ClusterNode> newTopology) {
                assert newStatus != null : "New status is null.";
                assert oldStatus != null : "Old status is null.";
                assert oldStatus != newStatus : "Both old and new statuses are the same [status=" + newStatus + ']';
                assert newTopology != null : "New topology is null.";

                if (DEBUG) {
                    log.debug("Processing gossip manager status change [old={}, new={}, order={}, topology={}]",
                        oldStatus, newStatus, order, newTopology);
                }

                if (gossipSpy != null) {
                    gossipSpy.onStatusChange(oldStatus, newStatus, order, newTopology);
                }

                switch (newStatus) {
                    case JOINING: {
                        if (DEBUG) {
                            log.debug("Cancelling a periodic join task.");
                        }

                        joinTask.cancel(false);

                        runOnServiceThread(() -> {
                            guard.lockRead();

                            try {
                                if (guard.isInitialized()) {
                                    seedNodeMgr.suspendDiscovery();
                                }
                            } finally {
                                guard.unlockRead();
                            }
                        });

                        break;
                    }
                    case UP: {
                        assert order > 0 : "Join order must be above zero [order=" + order + ']';

                        ctx.getCluster().onJoin(order, newTopology).thenAcceptAsync(event -> {
                            if (event != null) {
                                guard.lockRead();

                                try {
                                    ClusterTopology topology = event.getTopology();

                                    if (guard.isInitialized()) {
                                        if (metricsCallback != null) {
                                            metricsCallback.onTopologyChange(topology);
                                        }

                                        if (isCoordinator(topology)) {
                                            startSeedNodeCleaner();
                                        }
                                    }
                                } catch (RuntimeException | Error e) {
                                    log.error("Got an unexpected runtime error.", e);
                                } finally {
                                    guard.unlockRead();
                                }
                            }
                        }, gossipThread);

                        break;
                    }
                    case LEAVING: {
                        // No-op.
                        break;
                    }
                    case DOWN: {
                        if (ctx.getState() == Hekate.State.LEAVING) {
                            if (DEBUG) {
                                log.debug("Stopping periodic gossiping.");
                            }

                            gossipTask.cancel(false);

                            if (DEBUG) {
                                log.debug("Stopping periodic heartbeats.");
                            }

                            if (heartbeatTask != null) {
                                heartbeatTask.cancel(false);
                            }

                            Collection<UpdateBase> gossips = gossipMgr.batchGossip(GossipPolicy.ON_DOWN);

                            if (gossips.isEmpty()) {
                                ctx.getCluster().onLeave();
                            } else {
                                // Send final gossip updates and notify context on leave once sending is done.
                                // ---------------------------------------------------------------------------------------------------------
                                // Note: we are sending each update on a dedicated connection in order to make sure that receiver side will
                                //       not drop this message from its receive buffer if it detects connection failure while performing
                                //       a write operation (in such case connection gets closed and all unread buffered data is lost too).
                                AtomicInteger enqueued = new AtomicInteger(gossips.size());

                                gossips.forEach(msg -> sendAndClose(msg, () -> {
                                    if (enqueued.decrementAndGet() == 0) {
                                        runOnServiceThread(() -> ctx.getCluster().onLeave());
                                    }
                                }));
                            }
                        }

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected status: " + newStatus);
                    }
                }
            }

            @Override
            public void onTopologyChange(Set<ClusterNode> oldTopology, Set<ClusterNode> newTopology) {
                if (gossipSpy != null) {
                    gossipSpy.onTopologyChange(oldTopology, newTopology);
                }

                ctx.getCluster().onTopologyChange(newTopology).thenAcceptAsync(event -> {
                    if (event != null) {
                        guard.lockRead();

                        try {
                            if (guard.isInitialized()) {
                                ClusterTopology topology = event.getTopology();

                                if (metricsCallback != null) {
                                    metricsCallback.onTopologyChange(topology);
                                }

                                if (isCoordinator(topology)) {
                                    startSeedNodeCleaner();
                                } else {
                                    seedNodeMgr.stopCleaning();
                                }

                                if (!event.getRemoved().isEmpty()) {
                                    checkSplitBrain(node);
                                }
                            }
                        } catch (RuntimeException | Error e) {
                            log.error("Got an unexpected runtime error.", e);
                        } finally {
                            guard.unlockRead();
                        }
                    }
                }, gossipThread);
            }

            @Override
            public void onKnownAddressesChange(Set<ClusterAddress> oldAddresses, Set<ClusterAddress> newAddresses) {
                Set<InetSocketAddress> addresses = newAddresses.stream().map(ClusterAddress::getSocket).collect(toSet());

                knownAddresses = Collections.unmodifiableSet(addresses);
            }

            @Override
            public void onNodeFailureSuspected(ClusterNode failed, GossipNodeStatus status) {
                if (log.isWarnEnabled()) {
                    log.warn("Node failure suspected [address={}, status={}]", failed, status);
                }

                if (gossipSpy != null) {
                    gossipSpy.onNodeFailureSuspected(failed, status);
                }
            }

            @Override
            public void onNodeFailureUnsuspected(ClusterNode node, GossipNodeStatus status) {
                if (log.isWarnEnabled()) {
                    log.warn("Failure suspicion removed from node [address={}, status={}]", node, status);
                }

                if (gossipSpy != null) {
                    gossipSpy.onNodeFailureUnsuspected(node, status);
                }
            }

            @Override
            public void onNodeFailure(ClusterNode failed, GossipNodeStatus status) {
                if (log.isErrorEnabled()) {
                    log.error("Removing failed node from cluster [address={}, status={}]", failed, status);
                }

                if (gossipSpy != null) {
                    gossipSpy.onNodeFailure(failed, status);
                }
            }

            @Override
            public void onNodeInconsistency(GossipNodeStatus gossipStatus) {
                if (gossipSpy != null) {
                    gossipSpy.onNodeInconsistency(gossipStatus);
                }

                Hekate.State state = ctx.getState();

                switch (state) {
                    case JOINING:
                    case UP: {
                        applySplitBrainPolicy();

                        break;
                    }
                    case LEAVING: {
                        // Safe to terminate since we are leaving anyway.
                        ctx.terminate();

                        break;
                    }
                    case DOWN:
                    case INITIALIZING:
                    case TERMINATING: {
                        // No-op.
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unexpected status: " + state);
                    }
                }
            }

            private void startSeedNodeCleaner() {
                seedNodeMgr.startCleaning(net, () -> knownAddresses);
            }
        };
    }

    private void validateAndProcessAsync(JoinRequest request) {
        assert request != null : "Request is null";

        ClusterNodeId joiningNodeId = request.getFrom().getId();

        // Check that request validation is not running yet for the joining node.
        if (!asyncJoinValidations.contains(joiningNodeId)) {
            // Add concurrent validations guard.
            asyncJoinValidations.add(joiningNodeId);

            // Run acceptance testing on the system thread.
            runOnServiceThread(() -> {
                guard.lockRead();

                try {
                    if (guard.isInitialized()) {
                        String rejectReason = validateJoiningNode(request.getFromNode());

                        // Run acceptance testing results processing on a gossip thread.
                        runOnGossipThread(() -> {
                            // Enable subsequent validations of the joining node.
                            asyncJoinValidations.remove(joiningNodeId);

                            guard.lockRead();

                            try {
                                if (guard.isInitialized()) {
                                    JoinReply reply;

                                    if (rejectReason == null) {
                                        reply = gossipMgr.processJoinRequest(request);
                                    } else {
                                        reply = gossipMgr.reject(request, rejectReason);

                                    }

                                    send(reply);
                                }
                            } catch (RuntimeException | Error e) {
                                log.error("Got an unexpected error while processing a node join request [request={}]", request, e);
                            } finally {
                                guard.unlockRead();
                            }
                        });
                    } else {
                        if (DEBUG) {
                            log.debug("Skipped join request acceptance testing since service is not started [request={}]", request);
                        }
                    }
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected error while processing a node join request [request={}]", request, e);
                } finally {
                    guard.unlockRead();
                }
            });
        }
    }

    private String validateJoiningNode(ClusterNode newNode) {
        if (DEBUG) {
            log.debug("Checking join validators [node={}]", newNode);
        }

        String rejectReason = null;

        for (ClusterJoinValidator acceptor : joinValidators) {
            rejectReason = acceptor.acceptJoin(newNode, ctx.getHekate());

            if (rejectReason != null) {
                if (DEBUG) {
                    log.debug("Rejected cluster join request [node={}, reason={}, validator={}]", newNode, rejectReason, acceptor);
                }

                break;
            }
        }

        if (DEBUG) {
            if (rejectReason == null) {
                log.debug("New node was accepted [node={}]", newNode);
            } else {
                log.debug("New node was rejected [node={}, reason={}]", newNode, rejectReason);
            }
        }

        return rejectReason;
    }

    private void checkSplitBrain(ClusterNode localNode) {
        if (splitBrainDetector != null) {
            if (splitBrainDetectorActive.compareAndSet(false, true)) {
                runOnServiceThread(() -> {
                    try {
                        if (DEBUG) {
                            log.debug("Checking for cluster split-brain [detector={}]", splitBrainDetector);
                        }

                        if (!splitBrainDetector.isValid(localNode)) {
                            if (log.isWarnEnabled()) {
                                log.warn("Split-brain detected.");
                            }

                            applySplitBrainPolicy();
                        }
                    } catch (RuntimeException | Error e) {
                        ctx.terminate(e);
                    } finally {
                        splitBrainDetectorActive.compareAndSet(true, false);
                    }
                });
            } else {
                if (DEBUG) {
                    log.debug("Skipped split-brain checking since it is already in progress.");
                }
            }
        }
    }

    private void applySplitBrainPolicy() {
        guard.lockRead();

        try {
            if (guard.isInitialized()) {
                switch (splitBrainAction) {
                    case REJOIN: {
                        if (log.isWarnEnabled()) {
                            log.warn("Rejoining due to cluster state inconsistency.");
                        }

                        ctx.rejoin();

                        break;
                    }
                    case TERMINATE: {
                        if (log.isErrorEnabled()) {
                            log.error("Terminating due to cluster state inconsistency.");
                        }

                        ctx.terminate();

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected policy: " + splitBrainAction);
                    }
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private void send(GossipMessage msg) {
        send(msg, null);
    }

    private void send(GossipMessage msg, Runnable callback) {
        if (msg != null) {
            commMgr.send(msg, callback);
        }
    }

    private void sendAndClose(GossipProtocol msg) {
        sendAndClose(msg, null);
    }

    private void sendAndClose(GossipProtocol msg, Runnable callback) {
        if (msg != null) {
            commMgr.sendAndClose(msg, callback);
        }
    }

    private boolean isCoordinator(ClusterTopology topology) {
        return topology.getFirst().equals(node);
    }

    private void runOnGossipThread(Runnable task) {
        gossipThread.execute(task);
    }

    private void runOnServiceThread(Runnable task) {
        serviceThread.execute(task);
    }

    private InitializationContext getRequiredContext() {
        InitializationContext localCtx = this.ctx;

        if (localCtx == null) {
            throw new IllegalStateException("Cluster service is not joined.");
        }

        return localCtx;
    }

    private static ScheduledFuture<?> scheduleOn(ScheduledExecutorService executor, Runnable task, long intervalMs) {
        return scheduleOn(executor, task, intervalMs, intervalMs);
    }

    private static ScheduledFuture<?> scheduleOn(ScheduledExecutorService executor, Runnable task, long delay, long intervalMs) {
        return executor.scheduleWithFixedDelay(task, delay, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public String toString() {
        return ToString.format(ClusterService.class, this);
    }
}
