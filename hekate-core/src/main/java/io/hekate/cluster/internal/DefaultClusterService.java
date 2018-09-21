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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterJoinRejectedException;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.ClusterServiceJmx;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.internal.gossip.GossipCommListener;
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
import io.hekate.core.internal.util.Jvm;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.service.ClusterServiceManager;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
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
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toSet;

public class DefaultClusterService implements ClusterService, ClusterServiceManager, DependentService, ConfigurableService,
    InitializingService, TerminatingService, NetworkConfigProvider, JmxSupport<ClusterServiceJmx> {
    private static final Logger log = LoggerFactory.getLogger(DefaultClusterService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final String PROTOCOL_ID = "hekate.cluster";

    private final long gossipInterval;

    private final int speedUpGossipSize;

    private final SeedNodeProvider seedNodeProvider;

    private final FailureDetector failureDetector;

    private final SplitBrainAction splitBrainAction;

    private final SplitBrainDetector splitBrainDetector;

    @ToStringIgnore
    private final AtomicBoolean splitBrainDetectorActive = new AtomicBoolean();

    @ToStringIgnore
    private final List<ClusterAcceptor> acceptors;

    @ToStringIgnore
    private final GossipListener gossipSpy;

    @ToStringIgnore
    private final StateGuard guard;

    @ToStringIgnore
    private final AtomicReference<ClusterNodeId> localNodeIdRef = new AtomicReference<>();

    @ToStringIgnore
    private final List<ClusterEventListener> listeners;

    @ToStringIgnore
    private final List<DeferredClusterListener> deferredListeners = synchronizedList(new ArrayList<>());

    @ToStringIgnore
    private ClusterAcceptManager acceptMgr;

    @ToStringIgnore
    private SeedNodeManager seedNodeMgr;

    @ToStringIgnore
    private GossipManager gossipMgr;

    @ToStringIgnore
    private NetworkService net;

    @ToStringIgnore
    private ClusterMetricsSink metricsSink;

    @ToStringIgnore
    private JmxService jmx;

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
    private String clusterName;

    @ToStringIgnore
    private volatile InitializationContext ctx;

    @ToStringIgnore
    private volatile GossipCommManager commMgr;

    @ToStringIgnore
    private volatile ClusterNode localNode;

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

        // Pre-configured (unmodifiable) event listeners.
        this.listeners = unmodifiableList(nullSafe(factory.getClusterListeners()).collect(toCollection(() -> {
            List<ClusterEventListener> listeners = new ArrayList<>();

            listeners.add(new ClusterEventLogger());

            return listeners;
        })));

        // Join acceptors.
        this.acceptors = nullSafe(factory.getAcceptors()).collect(toCollection(ArrayList::new));
    }

    @Override
    public void resolve(DependencyContext ctx) {
        clusterName = ctx.clusterName();

        net = ctx.require(NetworkService.class);

        jmx = ctx.optional(JmxService.class);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        Collection<ClusterAcceptor> customAcceptors = ctx.findComponents(ClusterAcceptor.class);

        acceptors.addAll(customAcceptors);
    }

    @Override
    public Collection<NetworkConnectorConfig<?>> configureNetwork() {
        NetworkConnectorConfig<GossipProtocol> netCfg = new NetworkConnectorConfig<>();

        netCfg.setProtocol(PROTOCOL_ID);
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

            ctx = initCtx;
            localNode = initCtx.localNode();
            localNodeIdRef.set(initCtx.localNode().id());

            // Register cluster listeners.
            listeners.forEach(listener -> ctx.cluster().addListener(listener));
            deferredListeners.forEach(deferred -> ctx.cluster().addListener(deferred.listener(), deferred.eventTypes()));

            // Prepare seed node manager.
            seedNodeMgr = new SeedNodeManager(initCtx.clusterName(), seedNodeProvider);

            // Prepare workers.
            gossipThread = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("ClusterGossip"));
            serviceThread = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("Cluster"));

            // Prepare accept manager.
            acceptMgr = new ClusterAcceptManager(acceptors, serviceThread);

            // Prepare gossip listener.
            GossipListener gossipListener = createGossipListener();

            // Prepare gossip manager.
            gossipMgr = new GossipManager(initCtx.clusterName(), localNode, speedUpGossipSize, failureDetector, gossipListener);

            // Prepare gossip communication manager.
            NetworkConnector<GossipProtocol> connector = net.connector(PROTOCOL_ID);

            commMgr = new GossipCommManager(connector, localNode.address(), new GossipCommListener() {
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

                @Override
                public void onConnectFailure(ClusterAddress node) {
                    processConnectFailure(node);
                }

                @Override
                public Optional<Throwable> onBeforeSend(GossipProtocol msg) {
                    return gossipListener.onBeforeSend(msg);
                }
            });

            // Prepare metrics sink.
            metricsSink = new ClusterMetricsSink(ctx.metrics());

            // Register JMX beans (optional).
            if (jmx != null) {
                jmx.register(this);

                jmx.register(failureDetector);
                jmx.register(seedNodeProvider);

                if (splitBrainDetector != null) {
                    jmx.register(splitBrainDetector);
                }
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void joinAsync() {
        guard.lockReadWithStateCheck();

        try {
            ctx.cluster().onStartJoining().thenAcceptAsync(proceed -> {
                if (proceed && guard.isInitialized()) {
                    if (log.isInfoEnabled()) {
                        log.info("Joining cluster [cluster={}, local-node={}]", ctx.clusterName(), ctx.localNode());
                    }

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
                                    address = localNode.address();
                                    localSeedNodeMgr = seedNodeMgr;
                                } else {
                                    // Stop since there was a concurrent leave/terminate event.
                                    return;
                                }
                            } finally {
                                guard.unlockRead();
                            }

                            try {
                                // Check if node is not in a split-brain mode before trying to join.
                                if (splitBrainDetector != null) {
                                    boolean valid = splitBrainDetector.isValid(localNode);

                                    if (!valid) {
                                        // Try to schedule a new join attempt.
                                        guard.withReadLockIfInitialized(() -> {
                                            log.warn("Split-brain detected ...will wait for {} ms before making another attempt "
                                                + "[split-brain-detector={}]", gossipInterval, splitBrainDetector);

                                            serviceThread.schedule(this, gossipInterval, TimeUnit.MILLISECONDS);
                                        });

                                        return;
                                    }
                                }

                                // Start seed nodes discovery.
                                try {
                                    localSeedNodeMgr.startDiscovery(address.socket());
                                } catch (HekateException e) {
                                    // Try to schedule a new join attempt.
                                    boolean scheduled = guard.withReadLock(() -> {
                                        // Check that there were no concurrent leave/terminate events.
                                        if (guard.isInitialized()) {
                                            log.error("Failed to start seed nodes discovery "
                                                + "...will wait for {}ms before making another attempt.", gossipInterval, e);

                                            serviceThread.schedule(this, gossipInterval, TimeUnit.MILLISECONDS);

                                            return true;
                                        } else {
                                            return false;
                                        }
                                    });

                                    if (!scheduled) {
                                        // Make sure that seed nodes discovery is stopped in case of concurrent service termination.
                                        localSeedNodeMgr.stopDiscovery(address.socket());
                                    }

                                    return;
                                }

                                // Initialize failure detector.
                                if (DEBUG) {
                                    log.debug("Initializing failure detector [address={}]", address);
                                }

                                failureDetector.initialize(() -> address);

                                if (DEBUG) {
                                    log.debug("Initialized failure detector [address={}]", address);
                                }

                                // Schedule asynchronous join task.
                                if (!scheduleAsyncJoin()) {
                                    // Operation was rejected.
                                    // Stop components (in unlocked context in order to prevent thread blocking).
                                    if (DEBUG) {
                                        log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                                    }

                                    // Make sure that seed nodes discovery is stopped.
                                    localSeedNodeMgr.stopDiscovery(address.socket());

                                    // Make sure that failure detector is terminated.
                                    try {
                                        failureDetector.terminate();
                                    } catch (RuntimeException | Error e) {
                                        log.error("Got an unexpected runtime error during the failure detector termination.", e);
                                    }
                                }
                            } catch (HekateException | RuntimeException | Error e) {
                                ctx.terminate(e);
                            }
                        }
                    };

                    repeatableJoinTask.run();
                }
            }, serviceThread);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void preTerminate() {
        guard.withReadLockIfInitialized(() -> {
            if (ctx.state() == Hekate.State.LEAVING) {
                runOnGossipThread(this::doLeave);
            }
        });
    }

    @Override
    public void terminate() throws HekateException {
        List<Waiting> waiting = new ArrayList<>();

        guard.withWriteLock(() -> {
            if (guard.becomeTerminated()) {
                acceptMgr.terminate();

                if (seedNodeMgr != null) {
                    waiting.add(seedNodeMgr.stopCleaning());

                    InetSocketAddress localAddress = localNode.socket();

                    SeedNodeManager localSeedNodeMgr = seedNodeMgr;

                    waiting.add(() -> localSeedNodeMgr.stopDiscovery(localAddress));
                }

                waiting.add(AsyncUtils.shutdown(gossipThread));
                waiting.add(AsyncUtils.shutdown(serviceThread));

                if (commMgr != null) {
                    GossipCommManager localCommMgr = commMgr;

                    waiting.add(localCommMgr::stop);
                }

                if (failureDetector != null) {
                    waiting.add(failureDetector::terminate);
                }

                localNodeIdRef.set(null);

                localNode = null;
                commMgr = null;
                gossipMgr = null;
                acceptMgr = null;
                serviceThread = null;
                gossipThread = null;
                seedNodeMgr = null;
                metricsSink = null;
            }
        });

        Waiting.awaitAll(waiting).awaitUninterruptedly();
    }

    @Override
    public ClusterTopology topology() {
        return requireContext().cluster().topology();
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

        guard.withReadLock(() -> {
            if (guard.isInitialized()) {
                requireContext().cluster().addListener(listener);
            } else {
                deferredListeners.add(new DeferredClusterListener(listener, eventTypes));
            }
        });
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        guard.withReadLock(() -> {
            if (guard.isInitialized()) {
                requireContext().cluster().removeListener(listener);
            }

            deferredListeners.remove(new DeferredClusterListener(listener, null));
        });
    }

    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        ArgAssert.notNull(predicate, "Predicate");

        return guard.withReadLock(() -> {
            // Completable future that gets completed upon a cluster event that has a matching topology.
            class PredicateFuture extends CompletableFuture<ClusterTopology> implements ClusterEventListener {
                public PredicateFuture() {
                    // Unregister listener when this future gets completed.
                    whenComplete((topology, err) ->
                        removeListener(this)
                    );
                }

                @Override
                public void onEvent(ClusterEvent event) throws HekateException {
                    if (!isDone()) {
                        if (predicate.test(event.topology())) {
                            complete(event.topology());
                        } else if (event.type() == ClusterEventType.LEAVE) {
                            cancel(false);
                        }
                    }
                }
            }

            PredicateFuture future = new PredicateFuture();

            if (guard.isInitialized()) {
                requireContext().cluster().addListenerAsync(future);
            } else {
                deferredListeners.add(new DeferredClusterListener(future, null));
            }

            return future;
        });
    }

    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate) {
        return awaitFor(predicate, Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate, long timeout, TimeUnit timeUnit) {
        ArgAssert.notNull(predicate, "Predicate");

        // Fast try against the current topology.
        ClusterTopology immediateTopology = tryTopology(predicate);

        if (immediateTopology != null) {
            // Complete immediately.
            return true;
        } else {
            // Await via future object.
            Future<?> future = guard.withReadLock(() ->
                guard.isInitialized() ? futureOf(predicate) : null
            );

            if (future == null) {
                return false;
            } else {
                try {
                    future.get(timeout, timeUnit);

                    return true;
                } catch (InterruptedException | TimeoutException e) {
                    // Notify that this future is not needed anymore.
                    future.cancel(false);

                    return false;
                } catch (CancellationException | ExecutionException e) {
                    return false;
                }
            }
        }
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public ClusterNode localNode() {
        ClusterNode node = this.localNode;

        if (node == null) {
            throw new IllegalStateException(ClusterService.class.getSimpleName() + " is not initialized.");
        }

        return node;
    }

    public SeedNodeProvider seedNodeProvider() {
        return seedNodeProvider;
    }

    public FailureDetector failureDetector() {
        return failureDetector;
    }

    public SplitBrainDetector splitBrainDetector() {
        return splitBrainDetector;
    }

    public SplitBrainAction splitBrainAction() {
        return splitBrainAction;
    }

    public List<ClusterAcceptor> acceptors() {
        return unmodifiableList(acceptors);
    }

    @Override
    public ClusterServiceJmx jmx() {
        return new DefaultClusterServiceJmx(this);
    }

    private boolean scheduleAsyncJoin() {
        return guard.withWriteLock(() -> {
            if (guard.isInitialized()) {
                if (DEBUG) {
                    log.debug("Scheduling a periodic gossip task [interval={}]", gossipInterval);
                }

                // Schedule gossip task.
                gossipTask = scheduleOn(gossipThread, DefaultClusterService.this::gossip, gossipInterval);

                // Schedule heartbeat task.
                long hbInterval = failureDetector.heartbeatInterval();

                if (hbInterval > 0) {
                    if (DEBUG) {
                        log.debug("Scheduling a periodic heartbeat task [interval={}]", hbInterval);
                    }

                    heartbeatTask = scheduleOn(gossipThread, DefaultClusterService.this::heartbeat, hbInterval);
                }

                if (DEBUG) {
                    log.debug("Scheduling an asynchronous join task [interval={}]", gossipInterval);
                }

                // Schedule task for asynchronous join.
                joinTask = scheduleOn(serviceThread, DefaultClusterService.this::doJoin, 0, gossipInterval);

                return true;
            } else {
                return false;
            }
        });
    }

    private void doJoin() {
        guard.withReadLockIfInitialized(() -> {
            try {
                List<InetSocketAddress> nodes = seedNodeMgr.getSeedNodes();

                // Schedule join task to run on the gossip thread.
                runOnGossipThread(() ->
                    guard.withReadLockIfInitialized(() -> {
                        JoinRequest msg = gossipMgr.join(nodes);

                        if (msg != null && log.isInfoEnabled()) {
                            log.info("Sending cluster join request [seed-node={}].", msg.toAddress());
                        }

                        sendAndDisconnect(msg);
                    })
                );
            } catch (HekateException e) {
                log.error("Failed to obtain seed nodes ...will wait for {} ms before trying another attempt.", gossipInterval, e);
            }
        });
    }

    private void doLeave() {
        guard.withReadLockIfInitialized(() -> {
            UpdateBase msg = gossipMgr.leave();

            if (msg == null) {
                // Do not need to go through the cluster leave protocol (gossip manager decision).
                ctx.cluster().onLeave();
            } else {
                if (log.isInfoEnabled()) {
                    log.info("Leaving cluster...");
                }

                send(msg);
            }
        });
    }

    private void gossip() {
        guard.withReadLockIfInitialized(() ->
            gossipMgr.batchGossip(GossipPolicy.RANDOM_PREFER_UNSEEN).forEach(this::send)
        );
    }

    private void heartbeat() {
        guard.withReadLockIfInitialized(() -> {
            // Check nodes aliveness.
            boolean failureDetected = gossipMgr.checkAliveness();

            // Send heartbeats first (even if new failures were detected).
            Collection<ClusterAddress> targets = failureDetector.heartbeatTick();

            if (targets != null) {
                targets.stream()
                    .map(to -> new HeartbeatRequest(localNode.address(), to))
                    .forEach(this::send);
            }

            // Send gossip messages if new failures were detected.
            if (failureDetected) {
                gossip();
            }
        });
    }

    private void process(GossipProtocol msg) {
        assert msg != null : "Message is null.";

        guard.withReadLockIfInitialized(() -> {
            metricsSink.onGossipMessage(msg.type());

            if (msg instanceof GossipMessage) {
                GossipMessage gossipMsg = (GossipMessage)msg;

                if (!localNode.address().equals(gossipMsg.to())) {
                    if (DEBUG) {
                        log.debug("Ignored message since it is not addressed to the local node [message={}, node={}]", msg, localNode);
                    }

                    return;
                }
            }

            GossipProtocol.Type type = msg.type();

            if (type == GossipProtocol.Type.HEARTBEAT_REQUEST) {
                boolean reply = failureDetector.onHeartbeatRequest(msg.from());

                if (reply) {
                    send(new HeartbeatReply(localNode.address(), msg.from()));
                }
            } else if (type == GossipProtocol.Type.HEARTBEAT_REPLY) {
                failureDetector.onHeartbeatReply(msg.from());
            } else {
                runOnGossipThread(() ->
                    doProcess(msg)
                );
            }
        });
    }

    private void doProcess(GossipProtocol msg) {
        guard.withReadLockIfInitialized(() -> {
            switch (msg.type()) {
                case GOSSIP_UPDATE:
                case GOSSIP_UPDATE_DIGEST: {
                    UpdateBase update = (UpdateBase)msg;

                    GossipMessage reply = gossipMgr.processUpdate(update);

                    send(reply);

                    break;
                }
                case JOIN_REQUEST: {
                    JoinRequest request = (JoinRequest)msg;

                    // Check that the join request can be accepted.
                    JoinReject reject = gossipMgr.acceptJoinRequest(request);

                    if (reject == null) {
                        // Asynchronously validate and process the join request so that it won't block the gossiping thread.
                        acceptMgr.check(request.fromNode(), ctx.hekate()).thenAcceptAsync(rejectReason -> {
                            try {
                                guard.withReadLockIfInitialized(() -> {
                                    JoinReply reply;

                                    if (rejectReason.isPresent()) {
                                        reply = gossipMgr.reject(request, rejectReason.get());

                                    } else {
                                        reply = gossipMgr.processJoinRequest(request);
                                    }

                                    send(reply);
                                });
                            } catch (RuntimeException | Error e) {
                                fatalError(e);
                            }
                        }, gossipThread);
                    } else {
                        // Immediate reject.
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

                    sendAndDisconnect(newRequest);

                    break;
                }
                case HEARTBEAT_REQUEST:
                case HEARTBEAT_REPLY:
                case LONG_TERM_CONNECT:
                default: {
                    throw new IllegalArgumentException("Unexpected message type: " + msg);
                }
            }
        });
    }

    private void processSendFailure(GossipProtocol msg, Throwable error) {
        assert msg != null : "Message is null.";
        assert error != null : "Error is null.";

        guard.withReadLockIfInitialized(() -> {
            if (msg.type() == GossipProtocol.Type.JOIN_REQUEST) {
                JoinRequest request = (JoinRequest)msg;

                runOnGossipThread(() ->
                    processJoinSendFailure(request, error)
                );
            } else {
                if (DEBUG) {
                    log.debug("Failed to sent gossip message [error={}, message={}]", error.toString(), msg);
                }
            }
        });
    }

    private void processConnectFailure(ClusterAddress address) {
        assert address != null : "Node address is null.";

        guard.withReadLockIfInitialized(() ->
            failureDetector.onConnectFailure(address)
        );
    }

    private void processJoinSendFailure(JoinRequest msg, Throwable cause) {
        guard.withReadLockIfInitialized(() -> {
            if (msg.type() == GossipProtocol.Type.JOIN_REQUEST) {
                if (DEBUG) {
                    log.debug("Processing join message send failure notification [message={}]", msg);
                }

                JoinRequest newReq = gossipMgr.processJoinFailure(msg, cause);

                sendAndDisconnect(newReq);
            }
        });
    }

    private GossipListener createGossipListener() {
        return new GossipListener() {
            // Volatile since can be accessed by a different thread in seed node cleaner.
            private volatile Set<InetSocketAddress> knownAddresses = Collections.emptySet();

            @Override
            public void onJoinReject(ClusterAddress rejectedBy, String reason) {
                if (ctx.state() == Hekate.State.JOINING) {
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

                        runOnServiceThread(() ->
                            guard.withReadLockIfInitialized(() ->
                                seedNodeMgr.suspendDiscovery()
                            )
                        );

                        break;
                    }
                    case UP: {
                        assert order > 0 : "Join order must be above zero [order=" + order + ']';

                        ctx.cluster().onJoin(order, newTopology).thenAcceptAsync(event -> {
                            if (event != null) {
                                try {
                                    guard.withReadLockIfInitialized(() -> {
                                        ClusterTopology topology = event.topology();

                                        metricsSink.onTopologyChange(topology);

                                        if (isCoordinator(topology)) {
                                            startSeedNodeCleaner();
                                        }
                                    });
                                } catch (RuntimeException | Error e) {
                                    fatalError(e);
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
                        if (ctx.state() == Hekate.State.LEAVING) {
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

                            Collection<UpdateBase> msgs = gossipMgr.batchGossip(GossipPolicy.ON_DOWN);

                            if (msgs.isEmpty()) {
                                ctx.cluster().onLeave();
                            } else {
                                // Send final gossip updates and notify context on leave once sending is done.
                                AtomicInteger enqueued = new AtomicInteger(msgs.size());

                                msgs.forEach(msg ->
                                    send(msg, () -> {
                                        if (enqueued.decrementAndGet() == 0) {
                                            runOnServiceThread(() ->
                                                ctx.cluster().onLeave()
                                            );
                                        }
                                    })
                                );
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

                ctx.cluster().onTopologyChange(newTopology).thenAcceptAsync(event -> {
                    if (event != null) {
                        guard.withReadLockIfInitialized(() -> {
                            try {
                                ClusterTopology topology = event.topology();

                                metricsSink.onTopologyChange(topology);

                                if (isCoordinator(topology)) {
                                    startSeedNodeCleaner();
                                } else {
                                    seedNodeMgr.stopCleaning();
                                }

                                if (!event.removed().isEmpty()) {
                                    checkSplitBrain(localNode);
                                }
                            } catch (RuntimeException | Error e) {
                                fatalError(e);
                            }
                        });
                    }
                }, gossipThread);
            }

            @Override
            public void onKnownAddressesChange(Set<ClusterAddress> oldAddresses, Set<ClusterAddress> newAddresses) {
                Set<InetSocketAddress> addresses = newAddresses.stream().map(ClusterAddress::socket).collect(toSet());

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
                if (log.isWarnEnabled()) {
                    log.warn("Removing failed node from cluster [address={}, status={}]", failed, status);
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

                Hekate.State state = ctx.state();

                switch (state) {
                    case JOINING:
                    case SYNCHRONIZING:
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
                    case INITIALIZED:
                    case TERMINATING: {
                        // No-op.
                        break;
                    }
                    default: {
                        throw new IllegalStateException("Unexpected status: " + state);
                    }
                }
            }

            @Override
            public Optional<Throwable> onBeforeSend(GossipProtocol msg) {
                if (gossipSpy != null) {
                    return gossipSpy.onBeforeSend(msg);
                }

                return Optional.empty();
            }

            private void startSeedNodeCleaner() {
                seedNodeMgr.startCleaning(net, () -> knownAddresses);
            }
        };
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
                    } finally {
                        splitBrainDetectorActive.compareAndSet(true, false);
                    }
                });
            }
        }
    }

    private void applySplitBrainPolicy() {
        guard.withReadLockIfInitialized(() -> {
            switch (splitBrainAction) {
                case REJOIN: {
                    if (log.isWarnEnabled()) {
                        log.warn("Rejoining due to inconsistency of the cluster state.");
                    }

                    ctx.rejoin();

                    break;
                }
                case TERMINATE: {
                    if (log.isErrorEnabled()) {
                        log.error("Terminating due to inconsistency of the cluster state.");
                    }

                    ctx.terminate();

                    break;
                }
                case KILL_JVM: {
                    if (log.isErrorEnabled()) {
                        log.error("Killing the JVM due to inconsistency of the cluster state.");
                    }

                    Jvm.exit(250);

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected policy: " + splitBrainAction);
                }
            }
        });
    }

    private void send(GossipMessage msg) {
        send(msg, null);
    }

    private void send(GossipMessage msg, Runnable onComplete) {
        if (msg != null) {
            commMgr.send(msg, onComplete);
        }
    }

    private void sendAndDisconnect(GossipProtocol msg) {
        sendAndDisconnect(msg, null);
    }

    private void sendAndDisconnect(GossipProtocol msg, Runnable callback) {
        if (msg != null) {
            commMgr.sendAndDisconnect(msg, callback);
        }
    }

    private boolean isCoordinator(ClusterTopology topology) {
        return topology.first().equals(localNode);
    }

    private InitializationContext requireContext() {
        InitializationContext localCtx = this.ctx;

        if (localCtx == null) {
            throw new IllegalStateException("Cluster service is not initialized.");
        }

        return localCtx;
    }

    private ClusterTopology tryTopology(Predicate<ClusterTopology> predicate) {
        ClusterTopology topology = guard.withReadLock(() ->
            guard.isInitialized() ? topology() : null
        );

        if (topology != null && predicate.test(topology)) {
            return topology;
        } else {
            return null;
        }
    }

    private void runOnGossipThread(Runnable task) {
        gossipThread.execute(() -> {
            try {
                task.run();
            } catch (RuntimeException | Error e) {
                fatalError(e);
            }
        });
    }

    private void runOnServiceThread(Runnable task) {
        serviceThread.execute(() -> {
            try {
                task.run();
            } catch (RuntimeException | Error e) {
                fatalError(e);
            }
        });
    }

    private ScheduledFuture<?> scheduleOn(ScheduledExecutorService executor, Runnable task, long intervalMs) {
        return scheduleOn(executor, task, intervalMs, intervalMs);
    }

    private ScheduledFuture<?> scheduleOn(ScheduledExecutorService executor, Runnable task, long delay, long intervalMs) {
        return executor.scheduleWithFixedDelay(() -> {
            try {
                task.run();
            } catch (RuntimeException | Error e) {
                fatalError(e);
            }
        }, delay, intervalMs, TimeUnit.MILLISECONDS);
    }

    private void fatalError(Throwable e) {
        log.error("Got an unexpected runtime error.", e);

        InitializationContext localCtx = this.ctx;

        if (localCtx != null) {
            localCtx.terminate(e);
        }
    }

    @Override
    public String toString() {
        return ToString.format(ClusterService.class, this);
    }
}
