/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.core.internal;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveReason;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterNodeBuilder;
import io.hekate.cluster.internal.DefaultClusterNodeRuntime;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.internal.DefaultCodecService;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.HekateJmx;
import io.hekate.core.HekateVersion;
import io.hekate.core.InitializationFuture;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.TerminateFuture;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.report.DefaultConfigReporter;
import io.hekate.core.resource.ResourceService;
import io.hekate.core.service.ClusterContext;
import io.hekate.core.service.ClusterServiceManager;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.NetworkBindCallback;
import io.hekate.core.service.NetworkServiceManager;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.core.service.internal.ServiceManager;
import io.hekate.election.ElectionService;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingService;
import io.hekate.network.NetworkServerFailure;
import io.hekate.network.NetworkService;
import io.hekate.rpc.RpcService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.INITIALIZED;
import static io.hekate.core.Hekate.State.INITIALIZING;
import static io.hekate.core.Hekate.State.JOINING;
import static io.hekate.core.Hekate.State.LEAVING;
import static io.hekate.core.Hekate.State.SYNCHRONIZING;
import static io.hekate.core.Hekate.State.TERMINATING;
import static io.hekate.core.Hekate.State.UP;
import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

class HekateNode implements Hekate, JmxSupport<HekateJmx> {
    private static final Logger log = LoggerFactory.getLogger(HekateNode.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    private final String clusterName;

    private final Set<String> roles;

    private final Map<String, String> props;

    private final boolean report;

    private final PluginManager plugins;

    private final ServiceManager services;

    private final NetworkService network;

    private final NetworkServiceManager networkMgr;

    private final ClusterService cluster;

    private final ClusterServiceManager clusterMgr;

    private final ClusterEventManager clusterEvents;

    private final MessagingService messaging;

    private final LockService locks;

    private final ElectionService election;

    private final CoordinationService coordination;

    private final CodecService codec;

    private final RpcService rpc;

    private final MeterRegistry metrics;

    private final StateGuard guard = new StateGuard(Hekate.class);

    private final Map<String, Object> attributes = synchronizedMap(new HashMap<>());

    private final HekateLifecycle lifecycle = new HekateLifecycle(this);

    private boolean preTerminated;

    private volatile DefaultClusterTopology topology;

    private volatile ScheduledExecutorService sysWorker;

    private volatile DefaultClusterNode node;

    private volatile State state = DOWN;

    public HekateNode(HekateBootstrap boot) {
        ArgAssert.notNull(boot, "Bootstrap");

        // Install plugins.
        plugins = new PluginManager(boot);

        plugins.install();

        // Check configuration.
        ConfigCheck check = ConfigCheck.get(HekateBootstrap.class);

        check.notEmpty(boot.getClusterName(), "cluster name");
        check.validSysName(boot.getClusterName(), "cluster name");

        check.validSysName(boot.getNodeName(), "node name");

        check.notNull(boot.getDefaultCodec(), "default codec");
        check.isFalse(boot.getDefaultCodec().createCodec().isStateful(), "default codec can't be stateful.");

        // Basic properties.
        this.name = boot.getNodeName() != null ? boot.getNodeName().trim() : "";
        this.clusterName = boot.getClusterName().trim();
        this.report = boot.isConfigReport();

        // Node roles.
        Set<String> roles;

        if (boot.getRoles() == null) {
            roles = emptySet();
        } else {
            // Filter out nulls and trim non-null values.
            roles = unmodifiableSet(nullSafe(boot.getRoles()).map(String::trim).collect(toSet()));
        }

        // Node properties.
        Map<String, String> props = new HashMap<>();

        if (boot.getProperties() != null) {
            boot.getProperties().forEach((k, v) -> {
                // Trim non-null property keys and values.
                String key = k == null ? null : k.trim();
                String value = v == null ? null : v.trim();

                props.put(key, value);
            });
        }

        // Node properties from providers.
        if (boot.getPropertyProviders() != null) {
            nullSafe(boot.getPropertyProviders()).forEach(provider -> {
                Map<String, String> providerProps = provider.getProperties();

                if (providerProps != null && !providerProps.isEmpty()) {
                    providerProps.forEach((k, v) -> {
                        // Trim non-null property keys and values.
                        String key = k == null ? null : k.trim();
                        String value = v == null ? null : v.trim();

                        props.put(key, value);
                    });
                }
            });
        }

        // Node roles/properties.
        this.roles = unmodifiableSet(roles);
        this.props = unmodifiableMap(props);

        // Lifecycle listeners.
        nullSafe(boot.getLifecycleListeners()).forEach(lifecycle::add);

        // Cluster event manager.
        clusterEvents = new ClusterEventManager(this);

        // Metrics.
        metrics = boot.getMetrics() == null ? new SimpleMeterRegistry() : boot.getMetrics();

        // Service manager.
        services = createServiceManager(boot.getDefaultCodec(), boot.getServices());

        // Instantiate services.
        services.instantiate();

        // Get internal service managers.
        networkMgr = services.findService(NetworkServiceManager.class);
        clusterMgr = services.findService(ClusterServiceManager.class);

        check.notNull(networkMgr, NetworkServiceManager.class.getName(), "not found");
        check.notNull(clusterMgr, ClusterServiceManager.class.getName(), "not found");

        // Cache core services.
        codec = services.findService(CodecService.class);
        cluster = services.findService(ClusterService.class);
        messaging = services.findService(MessagingService.class);
        network = services.findService(NetworkService.class);
        locks = services.findService(LockService.class);
        election = services.findService(ElectionService.class);
        coordination = services.findService(CoordinationService.class);
        rpc = services.findService(RpcService.class);
    }

    @Override
    public ClusterNode localNode() {
        ClusterNode node = this.node;

        if (node == null) {
            throw new IllegalStateException(Hekate.class.getSimpleName() + " is not initialized.");
        }

        return node;
    }

    @Override
    public ClusterService cluster() {
        return cluster;
    }

    @Override
    public RpcService rpc() {
        return rpc;
    }

    @Override
    public MessagingService messaging() {
        return messaging;
    }

    @Override
    public LockService locks() {
        return locks;
    }

    @Override
    public ElectionService election() {
        return election;
    }

    @Override
    public CoordinationService coordination() {
        return coordination;
    }

    @Override
    public NetworkService network() {
        return network;
    }

    @Override
    public CodecService codec() {
        return codec;
    }

    @Override
    public InitializationFuture initializeAsync() {
        lifecycle.checkReentrancy();

        if (DEBUG) {
            log.debug("Initializing...");
        }

        return guard.withWriteLock(() -> {
            // Try to become INITIALIZING.
            switch (state) {
                case INITIALIZING:
                case INITIALIZED:
                case JOINING:
                case SYNCHRONIZING:
                case UP: {
                    if (DEBUG) {
                        log.debug("Skipped initialization request since already in {} state.", state);
                    }

                    return lifecycle.initFuture().fork();
                }
                case LEAVING:
                case TERMINATING: {
                    throw new IllegalStateException(Hekate.class.getSimpleName() + " is in " + state + " state.");
                }
                case DOWN: {
                    // Start initialization.
                    if (log.isInfoEnabled()) {
                        // Print version.
                        log.info("{}.", HekateVersion.info());
                    }

                    // Generate new ID for this node.
                    ClusterNodeId id = new ClusterNodeId();

                    // Initialize asynchronous task executor.
                    sysWorker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("Sys", name));

                    // Initialize cluster event manager.
                    clusterEvents.start(new HekateThreadFactory("ClusterEvent", name));

                    // Switch to the initializing state.
                    become(INITIALIZING);

                    // Schedule asynchronous initialization.
                    runOnSysThread(() ->
                        selectAddressAndBind(id)
                    );

                    return lifecycle.initFuture().fork();
                }
                default: {
                    throw new IllegalArgumentException("Unexpected state: " + state);
                }
            }
        });
    }

    @Override
    public Hekate initialize() throws InterruptedException, HekateFutureException {
        return initializeAsync().get();
    }

    @Override
    public JoinFuture joinAsync() {
        lifecycle.checkReentrancy();

        JoinFuture future = guard.withReadLock(lifecycle::joinFuture);

        initializeAsync().thenRun(() ->
            runOnSysThread(() ->
                guard.withWriteLock(() -> {
                    try {
                        if (state == INITIALIZED) {
                            become(JOINING);

                            clusterMgr.joinAsync();
                        }
                    } catch (RuntimeException | Error e) {
                        doTerminateAsync(ClusterLeaveReason.TERMINATE, e);
                    }
                })
            )
        );

        return future.fork();
    }

    @Override
    public Hekate join() throws HekateFutureException, InterruptedException {
        return joinAsync().get();
    }

    @Override
    public LeaveFuture leaveAsync() {
        lifecycle.checkReentrancy();

        if (DEBUG) {
            log.debug("Leaving...");
        }

        return guard.withWriteLock(() -> {
            LeaveFuture future;

            switch (state) {
                case INITIALIZING:
                case INITIALIZED: {
                    future = lifecycle.leaveFuture().fork();

                    // Safe to run termination process (and bypass leave protocol)
                    // since we haven't started joining yet.
                    doTerminateAsync(ClusterLeaveReason.LEAVE);

                    break;
                }
                case JOINING:
                case SYNCHRONIZING:
                case UP: {
                    future = lifecycle.leaveFuture().fork();

                    if (DEBUG) {
                        log.debug("Scheduling leave task for asynchronous processing.");
                    }

                    become(LEAVING);

                    runOnSysThread(this::doLeave);

                    break;
                }
                case LEAVING:
                case TERMINATING: {
                    future = lifecycle.leaveFuture().fork();

                    // Make sure that rejoining will not take place after the ongoing termination process.
                    // We do it because of this is an explicitly requested leave operation
                    // which has higher priority over automatic internal rejoins.
                    lifecycle.cancelRejoin();

                    if (DEBUG) {
                        log.debug("Skipped leave request since already in {} state.", state);
                    }

                    break;
                }
                case DOWN: {
                    future = LeaveFuture.completed(this);

                    if (DEBUG) {
                        log.debug("Skipped leave request since already in {} state.", state);
                    }

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected state: " + state);
                }
            }

            return future;
        });
    }

    @Override
    public Hekate leave() throws InterruptedException, HekateFutureException {
        return leaveAsync().get();
    }

    @Override
    public TerminateFuture terminateAsync() {
        lifecycle.checkReentrancy();

        return doTerminateAsync(ClusterLeaveReason.TERMINATE);
    }

    @Override
    public Hekate terminate() throws InterruptedException, HekateFutureException {
        return terminateAsync().get();
    }

    @Override
    public <T extends Service> boolean has(Class<T> type) {
        return services.findService(type) != null;
    }

    @Override
    public <T extends Service> T get(Class<T> type) {
        T service = services.findService(type);

        if (service == null) {
            throw new IllegalArgumentException("No such service: " + type.getName());
        }

        return service;
    }

    @Override
    public Set<Class<? extends Service>> services() {
        return services.getServiceTypes();
    }

    @Override
    public State state() {
        return state;
    }

    @Override
    public Object setAttribute(String name, Object value) {
        ArgAssert.notNull(name, "Attribute name");

        if (value == null) {
            return attributes.remove(name);
        } else {
            return attributes.put(name, value);
        }
    }

    @Override
    public Object getAttribute(String name) {
        return attributes.get(name);
    }

    @Override
    public void addListener(LifecycleListener listener) {
        lifecycle.add(listener);
    }

    @Override
    public boolean removeListener(LifecycleListener listener) {
        return lifecycle.remove(listener);
    }

    @Override
    public Hekate hekate() {
        return this;
    }

    @Override
    public HekateJmx jmx() {
        return new HekateNodeJmx(this);
    }

    private void selectAddressAndBind(ClusterNodeId id) {
        guard.lockWrite();

        try {
            // Make sure that we are still initializing with the same node identifier.
            // Need to perform this check in order to stop early in case of concurrent leave/termination events.
            if (state == INITIALIZING) {
                // Bind network service.
                networkMgr.bind(new NetworkBindCallback() {
                    @Override
                    public void onBind(InetSocketAddress address) {
                        guard.withReadLock(() -> {
                            if (state == INITIALIZING) {
                                // Continue initialization on the system thread.
                                runOnSysThread(() ->
                                    doInitializeNode(new ClusterAddress(address, id))
                                );
                            } else {
                                if (DEBUG) {
                                    log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                                }
                            }
                        });
                    }

                    @Override
                    public NetworkServerFailure.Resolution onFailure(NetworkServerFailure failure) {
                        String msg = String.format(
                            "Failed to start network service [address=%s, reason=%s]",
                            failure.lastTriedAddress(),
                            failure.cause()
                        );

                        doTerminateAsync(ClusterLeaveReason.TERMINATE, new HekateException(msg, failure.cause()));

                        return failure.fail();
                    }
                });
            } else {
                if (DEBUG) {
                    log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                }
            }
        } catch (HekateException | RuntimeException | Error e) {
            // Schedule termination while still holding the write lock.
            doTerminateAsync(ClusterLeaveReason.TERMINATE, e);
        } finally {
            guard.unlockWrite();
        }
    }

    private void doInitializeNode(ClusterAddress address) {
        guard.lockWrite();

        try {
            // Make sure that we are still initializing with the same node identifier.
            // Need to perform this check in order to stop early in case of concurrent leave/termination events.
            if (state == INITIALIZING) {
                if (DEBUG) {
                    log.debug("Initializing node...");
                }

                // Initialize node info.
                node = new DefaultClusterNodeBuilder()
                    .withAddress(address)
                    .withName(name)
                    .withLocalNode(true)
                    .withJoinOrder(DefaultClusterNode.NON_JOINED_ORDER)
                    .withRoles(roles)
                    .withProperties(props)
                    .withServices(services.getServicesInfo())
                    .withSysInfo(DefaultClusterNodeRuntime.getLocalInfo())
                    .createNode();

                // Prepare initial (empty) topology.
                DefaultClusterTopology oldTopology = this.topology;

                if (oldTopology == null) {
                    topology = DefaultClusterTopology.empty();
                } else {
                    // Inherit topology version in case of rejoin.
                    topology = oldTopology.update(emptySet());
                }

                // Initialize services.
                InitializationContext ctx = createInitContext();

                services.preInitialize(ctx);
                services.initialize(ctx);
                services.postInitialize(ctx);

                // Register to JMX (optional).
                JmxService jmx = services.findService(JmxService.class);

                if (jmx != null) {
                    jmx.register(this);
                }

                // Configuration report.
                if (report) {
                    DefaultConfigReporter report = new DefaultConfigReporter();

                    // Node details report.
                    report.section("node", r -> {
                        r.value("id", node.id());
                        r.value("address", node.address().socket());
                        r.value("name", node.name());
                        r.value("cluster", clusterName);
                        r.value("roles", node.roles());
                        r.value("properties", node.properties());
                        r.value("pid", node.runtime().pid());
                        r.value("cpu", node.runtime().cpus());
                        r.value("ram", Utils.byteSizeFormat(node.runtime().maxMemory()));
                        r.value("os", node.runtime().osName());
                        r.value("jvm", node.runtime().jvmName()
                            + " - " + node.runtime().jvmVersion()
                            + " (" + node.runtime().jvmVendor() + ")"
                        );
                    });

                    // Services report.
                    services.configReport(report);

                    // Log report.
                    log.info("Initialized node: {}", report.report());
                }

                // Start plugins.
                plugins.start(this);

                // Pre-check state because some plugin could initiate leave/termination procedure.
                if (state == INITIALIZING) {
                    become(INITIALIZED);

                    if (DEBUG) {
                        log.debug("Done initializing node.");
                    }
                }
            } else {
                if (DEBUG) {
                    log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                }
            }
        } catch (HekateException | RuntimeException | Error e) {
            // Schedule termination while still holding the write lock.
            doTerminateAsync(ClusterLeaveReason.TERMINATE, e);
        } finally {
            guard.unlockWrite();
        }
    }

    private InitializationContext createInitContext() {
        ClusterContext cluster = createClusterContext();

        return new InitializationContext() {
            @Override
            public String clusterName() {
                return clusterName;
            }

            @Override
            public State state() {
                return state;
            }

            @Override
            public ClusterContext cluster() {
                return cluster;
            }

            @Override
            public ClusterNode localNode() {
                return node;
            }

            @Override
            public Hekate hekate() {
                return HekateNode.this;
            }

            @Override
            public void rejoin() {
                doTerminateAsync(true, ClusterLeaveReason.SPLIT_BRAIN, null);
            }

            @Override
            public void terminate() {
                doTerminateAsync(ClusterLeaveReason.SPLIT_BRAIN);
            }

            @Override
            public void terminate(Throwable e) {
                doTerminateAsync(ClusterLeaveReason.TERMINATE, e);
            }

            @Override
            public MeterRegistry metrics() {
                return metrics;
            }

            @Override
            public String toString() {
                return ToString.format(InitializationContext.class, this);
            }
        };
    }

    private ClusterContext createClusterContext() {
        return new ClusterContext() {
            @Override
            public CompletableFuture<ClusterJoinEvent> onJoin(int joinOrder, Set<ClusterNode> nodes) {
                CompletableFuture<ClusterJoinEvent> future = new CompletableFuture<>();

                runOnSysThread(() ->
                    guard.withWriteLock(() -> {
                        if (state == JOINING) {
                            // Initialize the join order.
                            node.setJoinOrder(joinOrder);

                            // Update topology.
                            topology = topology().update(nodes);

                            if (DEBUG) {
                                log.debug("Updated local topology [topology={}]", topology);
                            }

                            if (log.isInfoEnabled()) {
                                log.info("Joined the cluster ...will synchronize [join-order={}, topology={}]", joinOrder, topology);
                            }

                            // Start synchronization.
                            become(SYNCHRONIZING);

                            // Fire the cluster join event.
                            ClusterJoinEvent event = new ClusterJoinEvent(topology, hekate());

                            clusterEvents.fireAsync(event).whenComplete((ignore, err) ->
                                // Switch to UP state.
                                runOnSysThread(() ->
                                    guard.withWriteLock(() -> {
                                        if (err == null) {
                                            // Check that state wasn't changed while we were waiting for synchronization.
                                            if (state == SYNCHRONIZING) {
                                                log.info("Hekate is UP and running [cluster={}, node={}]", clusterName, node);

                                                become(UP);
                                            }
                                        } else {
                                            // Terminate on error.
                                            doTerminateAsync(ClusterLeaveReason.TERMINATE, err);
                                        }
                                    })
                                )
                            );

                            future.complete(event);
                        } else {
                            future.complete(null);
                        }
                    })
                );

                return future;
            }

            @Override
            public CompletableFuture<ClusterChangeEvent> onTopologyChange(Set<ClusterNode> live, Set<ClusterNode> failed) {
                CompletableFuture<ClusterChangeEvent> future = new CompletableFuture<>();

                runOnSysThread(() ->
                    guard.withWriteLock(() -> {
                        DefaultClusterTopology prev = topology;
                        DefaultClusterTopology next = prev.updateIfModified(live);

                        if (clusterEvents.isJoinEventFired() && next.version() != prev.version()) {
                            // Update topology.
                            topology = next;

                            // Fire cluster event.
                            ClusterChangeEvent event = new ClusterChangeEvent(
                                prev,
                                next,
                                failed,
                                hekate()
                            );

                            if (log.isInfoEnabled()) {
                                log.info("Updated cluster topology [added={}, removed={}, failed={}, topology={}]",
                                    event.added(), event.removed(), event.failed(), event.topology());
                            }

                            clusterEvents.fireAsync(event).whenComplete((ignore, err) ->
                                future.complete(event)
                            );
                        } else {
                            future.complete(null);
                        }
                    })
                );

                return future;
            }

            @Override
            public void onLeave() {
                if (log.isInfoEnabled()) {
                    log.info("Done leaving cluster.");
                }

                doTerminateAsync(ClusterLeaveReason.LEAVE);
            }

            @Override
            public DefaultClusterTopology topology() {
                return topology;
            }

            @Override
            public void addListener(ClusterEventListener listener) {
                clusterEvents.addListener(listener);
            }

            @Override
            public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
                clusterEvents.addListener(listener, eventTypes);
            }

            @Override
            public void addListenerAsync(ClusterEventListener listener) {
                clusterEvents.addListenerAsync(listener);
            }

            @Override
            public void addListenerAsync(ClusterEventListener listener, ClusterEventType... eventTypes) {
                clusterEvents.addListenerAsync(listener, eventTypes);
            }

            @Override
            public void removeListener(ClusterEventListener listener) {
                clusterEvents.removeListener(listener);
            }
        };
    }

    private TerminateFuture doTerminateAsync(ClusterLeaveReason reason) {
        return doTerminateAsync(false, reason, null);
    }

    private TerminateFuture doTerminateAsync(ClusterLeaveReason reason, Throwable cause) {
        return doTerminateAsync(false, reason, cause);
    }

    private TerminateFuture doTerminateAsync(boolean rejoin, ClusterLeaveReason reason, Throwable cause) {
        return guard.withWriteLock(() -> {
            switch (state) {
                case INITIALIZING:
                case INITIALIZED:
                case JOINING:
                case SYNCHRONIZING:
                case UP:
                case LEAVING: {
                    if (DEBUG) {
                        log.debug("Scheduling task for asynchronous termination [rejoin={}]", rejoin);
                    }

                    become(TERMINATING);

                    // Schedule rejoining after termination.
                    if (rejoin) {
                        lifecycle.scheduleRejoin();
                    }

                    // Run termination on the system thread.
                    runOnSysThread(() ->
                        doTerminate(reason, cause)
                    );

                    return lifecycle.terminateFuture().fork();
                }
                case TERMINATING: {
                    if (DEBUG) {
                        log.debug("Skipped termination request processing since service is already in {} state.", TERMINATING);
                    }

                    // Prevent ongoing termination process from rejoining.
                    if (!rejoin) {
                        lifecycle.cancelRejoin();
                    }

                    return lifecycle.terminateFuture().fork();
                }
                case DOWN: {
                    if (DEBUG) {
                        log.debug("Skipped termination request processing since service is already in {} state.", DOWN);
                    }

                    // Already terminated.
                    return TerminateFuture.completed(this);
                }
                default: {
                    throw new IllegalArgumentException("Unexpected state: " + state);
                }
            }
        });
    }

    private void doLeave() {
        guard.withWriteLock(() -> {
            if (state == LEAVING) {
                clusterEvents.ensureLeaveEventFired(ClusterLeaveReason.LEAVE, topology).thenRun(() ->
                    runOnSysThread(() -> {
                        if (state == LEAVING) {
                            preTerminateServices();

                            clusterMgr.leaveAsync();
                        }
                    })
                );
            }
        });
    }

    private void preTerminateServices() {
        if (!preTerminated) {
            preTerminated = true;

            services.preTerminate();

            plugins.stop();
        }
    }

    private void doTerminate(ClusterLeaveReason reason, Throwable cause) {
        if (cause == null) {
            if (log.isInfoEnabled()) {
                log.info("Terminating...");
            }
        } else {
            if (log.isErrorEnabled()) {
                log.error("Terminating because of an unrecoverable error.", cause);
            }
        }

        try {
            AsyncUtils.getUninterruptedly(clusterEvents.ensureLeaveEventFired(reason, topology));
        } catch (ExecutionException e) {
            log.error("Got an unexpected error while waiting for cluster leave event processing.", e);
        }

        preTerminateServices();
        services.terminate();
        services.postTerminate();

        clusterEvents.stop();

        Runnable cleanup;

        guard.lockWrite();

        try {
            // Check if we should rejoin after termination.
            boolean rejoin = lifecycle.isRejoinScheduled();

            // Important to use shutdown NOW in order to prevent stale tasks execution.
            // Anyway, such tasks were supposed to be executed in the context of this node,
            // which is already terminated.
            sysWorker.shutdownNow();

            become(DOWN);

            cleanup = lifecycle.terminate(cause);

            sysWorker = null;
            preTerminated = false;

            if (log.isInfoEnabled()) {
                log.info("Terminated.");
            }

            // Start rejoining.
            if (rejoin) {
                joinAsync();
            }
        } finally {
            guard.unlockWrite();
        }

        cleanup.run();
    }

    private void become(State state) {
        assert guard.isWriteLocked() : "Must be locked for writing.";

        this.state = state;

        lifecycle.notifyStateChange();

        // Check if we should notify appropriate futures.
        if (state == INITIALIZED) {
            runOnSysThread(() ->
                lifecycle.initFuture().complete(this)
            );
        } else if (state == UP) {
            runOnSysThread(() ->
                lifecycle.joinFuture().complete(this)
            );
        }
    }

    private ServiceManager createServiceManager(CodecFactory<Object> codec, List<ServiceFactory<? extends Service>> services) {
        // Prepare built-in services.
        List<Service> builtInServices = singletonList(new DefaultCodecService(codec));

        // Prepare core services.
        List<Class<? extends Service>> coreServices = new ArrayList<>();

        coreServices.add(ResourceService.class);
        coreServices.add(NetworkService.class);
        coreServices.add(ClusterService.class);
        coreServices.add(MessagingService.class);
        coreServices.add(RpcService.class);
        coreServices.add(LockService.class);
        coreServices.add(ElectionService.class);
        coreServices.add(CoordinationService.class);

        // Prepare custom services.
        List<ServiceFactory<? extends Service>> factories = new ArrayList<>();

        // Filter out null values.
        nullSafe(services).forEach(factories::add);

        return new ServiceManager(
            name,
            clusterName,
            this,
            metrics,
            builtInServices,
            coreServices,
            factories
        );
    }

    private void runOnSysThread(Runnable task) {
        sysWorker.execute(() -> {
            try {
                task.run();
            } catch (RuntimeException | Error e) {
                log.error("Got an unexpected runtime error.", e);
            }
        });
    }

    @Override
    public String toString() {
        return Hekate.class.getSimpleName() + "[state=" + state + ", node=" + node + ']';
    }
}
