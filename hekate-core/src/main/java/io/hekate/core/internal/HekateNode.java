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

package io.hekate.core.internal;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterTopology;
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
import io.hekate.codec.JavaSerializable;
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
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxSupport;
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
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.NetworkServerFailure;
import io.hekate.network.NetworkService;
import io.hekate.rpc.RpcService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.INITIALIZED;
import static io.hekate.core.Hekate.State.INITIALIZING;
import static io.hekate.core.Hekate.State.JOINING;
import static io.hekate.core.Hekate.State.LEAVING;
import static io.hekate.core.Hekate.State.TERMINATING;
import static io.hekate.core.Hekate.State.UP;
import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedMap;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

class HekateNode implements Hekate, JavaSerializable, JmxSupport<HekateJmx> {
    static class SerializationHandle implements Serializable {
        static final SerializationHandle INSTANCE = new SerializationHandle();

        private static final long serialVersionUID = 1;

        protected Object readResolve() {
            return HekateCodecHelper.threadLocal();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HekateNode.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String nodeName;

    private final String clusterName;

    private final Set<String> nodeRoles;

    private final Map<String, String> nodeProps;

    private final NetworkServiceManager networkManager;

    private final ClusterServiceManager clusterManager;

    private final PluginManager plugins;

    private final ServiceManager services;

    private final StateGuard guard = new StateGuard(Hekate.class);

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    private final AtomicReference<Boolean> rejoining = new AtomicReference<>();

    private final AtomicReference<State> state = new AtomicReference<>(DOWN);

    private final AtomicReference<TerminateFuture> terminateFutureRef = new AtomicReference<>();

    private final Map<String, Object> attributes = synchronizedMap(new HashMap<>());

    private final ClusterEventManager clusterEvents;

    private final NetworkService network;

    private final ClusterService cluster;

    private final MessagingService messaging;

    private final LockService locks;

    private final ElectionService election;

    private final CoordinationService coordination;

    private final LocalMetricsService localMetrics;

    private final ClusterMetricsService clusterMetrics;

    private final CodecService codec;

    private final RpcService rpc;

    private boolean preTerminated;

    private InitializationFuture initFuture = new InitializationFuture();

    private JoinFuture joinFuture = new JoinFuture();

    private LeaveFuture leaveFuture = new LeaveFuture();

    private ClusterNodeId nodeId;

    private volatile DefaultClusterTopology topology;

    private volatile ScheduledExecutorService sysWorker;

    private volatile DefaultClusterNode node;

    public HekateNode(HekateBootstrap boot) {
        assert boot != null : "Bootstrap is null.";

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
        this.nodeName = boot.getNodeName() != null ? boot.getNodeName().trim() : "";
        this.clusterName = boot.getClusterName().trim();

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
        nodeRoles = unmodifiableSet(roles);
        nodeProps = unmodifiableMap(props);

        // Lifecycle listeners.
        nullSafe(boot.getLifecycleListeners()).forEach(listeners::add);

        // Cluster event manager.
        clusterEvents = new ClusterEventManager(this);

        // Service manager.
        services = createServiceManager(boot.getDefaultCodec(), boot.getServices());

        // Instantiate services.
        services.instantiate();

        // Get internal service managers.
        networkManager = services.findService(NetworkServiceManager.class);
        clusterManager = services.findService(ClusterServiceManager.class);

        check.notNull(networkManager, NetworkServiceManager.class.getName(), "not found");
        check.notNull(clusterManager, ClusterServiceManager.class.getName(), "not found");

        // Cache core services.
        codec = services.findService(CodecService.class);
        cluster = services.findService(ClusterService.class);
        messaging = services.findService(MessagingService.class);
        network = services.findService(NetworkService.class);
        locks = services.findService(LockService.class);
        election = services.findService(ElectionService.class);
        coordination = services.findService(CoordinationService.class);
        rpc = services.findService(RpcService.class);
        localMetrics = services.findService(LocalMetricsService.class);
        clusterMetrics = services.findService(ClusterMetricsService.class);
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
    public LocalMetricsService localMetrics() {
        return localMetrics;
    }

    @Override
    public ClusterMetricsService clusterMetrics() {
        return clusterMetrics;
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
        if (DEBUG) {
            log.debug("Initializing...");
        }

        return guard.withWriteLock(() -> {
            // Try to become INITIALIZING.
            switch (state.get()) {
                case DOWN: {
                    state.set(INITIALIZING);

                    break;
                }
                case INITIALIZING:
                case INITIALIZED:
                case JOINING:
                case UP: {
                    if (DEBUG) {
                        log.debug("Skipped initialization request since already in {} state.", state);
                    }

                    return initFuture.fork();
                }
                case LEAVING:
                case TERMINATING: {
                    throw new IllegalStateException(Hekate.class.getSimpleName() + " is in " + state + " state.");
                }
                default: {
                    throw new IllegalArgumentException("Unexpected state: " + state);
                }
            }

            // Generate new ID for this node.
            ClusterNodeId localNodeId = new ClusterNodeId();

            nodeId = localNodeId;

            // Initialize asynchronous task executor.
            sysWorker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory(nodeName, "Sys"));

            // Initialize cluster event manager.
            clusterEvents.start(new HekateThreadFactory(nodeName, "ClusterEvent"));

            notifyOnLifecycleChange();

            // Make sure that we are still initializing (lifecycle listener could request for leave/termination).
            if (isInitializingFor(localNodeId)) {
                // Schedule asynchronous initialization.
                runOnSysThread(() ->
                    selectAddressAndBind(localNodeId)
                );
            }

            return initFuture.fork();
        });
    }

    @Override
    public Hekate initialize() throws InterruptedException, HekateFutureException {
        return initializeAsync().get();
    }

    @Override
    public JoinFuture joinAsync() {
        JoinFuture localJoinFuture = guard.withReadLock(() -> this.joinFuture);

        initializeAsync().thenRun(() -> {
            try {
                if (state.get() == INITIALIZED) {
                    clusterManager.joinAsync();
                }
            } catch (RuntimeException | Error e) {
                if (log.isErrorEnabled()) {
                    log.error("Got and unexpected runtime error while joining the cluster.", e);
                }
            }
        });

        return localJoinFuture.fork();
    }

    @Override
    public Hekate join() throws HekateFutureException, InterruptedException {
        return joinAsync().get();
    }

    @Override
    public LeaveFuture leaveAsync() {
        if (DEBUG) {
            log.debug("Leaving...");
        }

        return guard.withWriteLock(() -> {
            if (state.get() == DOWN) {
                if (DEBUG) {
                    log.debug("Skipped leave request since already in {} state.", state);
                }

                // Not joined. Return a future that is immediately completed.
                LeaveFuture future = new LeaveFuture();

                future.complete(this);

                return future;
            } else if (state.get() == INITIALIZING || state.get() == INITIALIZED) {
                // Since we are still initializing it is safe to run termination process (and bypass leave protocol).
                doTerminateAsync(ClusterLeaveReason.LEAVE);

                return leaveFuture.fork();
            } else if (state.compareAndSet(JOINING, LEAVING) || state.compareAndSet(UP, LEAVING)) {
                notifyOnLifecycleChange();

                // Double check that state wasn't changed by listeners.
                if (state.get() == LEAVING) {
                    if (DEBUG) {
                        log.debug("Scheduling leave task for asynchronous processing.");
                    }

                    runOnSysThread(this::doLeave);
                }

                return leaveFuture.fork();
            } else /* <-- LEAVING or TERMINATING */ {
                if (DEBUG) {
                    log.debug("Skipped leave request since already in {} state.", state);
                }

                // Make sure that rejoining will not take place.
                rejoining.compareAndSet(true, false);

                return leaveFuture.fork();
            }
        });
    }

    @Override
    public Hekate leave() throws InterruptedException, HekateFutureException {
        return leaveAsync().get();
    }

    @Override
    public <T extends Service> boolean has(Class<T> type) {
        return services.findService(type) != null;
    }

    @Override
    public <T extends Service> T get(Class<T> type) {
        T service = services.findService(type);

        ArgAssert.check(service != null, "No such service: " + type.getName());

        return service;
    }

    @Override
    public Set<Class<? extends Service>> services() {
        return services.getServiceTypes();
    }

    @Override
    public State state() {
        return state.get();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A setAttribute(String name, Object value) {
        ArgAssert.notNull(name, "Attribute name");

        if (value == null) {
            return (A)attributes.remove(name);
        } else {
            return (A)attributes.put(name, value);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <A> A getAttribute(String name) {
        return (A)attributes.get(name);
    }

    @Override
    public TerminateFuture terminateAsync() {
        return doTerminateAsync(ClusterLeaveReason.TERMINATE);
    }

    @Override
    public Hekate terminate() throws InterruptedException, HekateFutureException {
        return terminateAsync().get();
    }

    @Override
    public void addListener(LifecycleListener listener) {
        ArgAssert.notNull(listener, "Listener");

        if (DEBUG) {
            log.debug("Adding lifecycle listener [listener={}]", listener);
        }

        listeners.add(listener);
    }

    @Override
    public boolean removeListener(LifecycleListener listener) {
        if (listener != null && listeners.remove(listener)) {
            if (DEBUG) {
                log.debug("Removed lifecycle listener [listener={}]", listener);
            }

            return true;
        }

        return false;
    }

    @Override
    public Hekate hekate() {
        return this;
    }

    @Override
    public HekateJmx jmx() {
        return new HekateNodeJmx(this);
    }

    protected Object writeReplace() throws ObjectStreamException {
        return SerializationHandle.INSTANCE;
    }

    private void selectAddressAndBind(ClusterNodeId localNodeId) {
        guard.lockWrite();

        try {
            // Make sure that we are still initializing with the same node identifier.
            // Need to perform this check in order to stop early in case of concurrent leave/termination events.
            if (isInitializingFor(localNodeId)) {
                if (log.isInfoEnabled()) {
                    log.info("Initializing {}.", HekateVersion.info());
                }

                // Bind network service.
                networkManager.bind(new NetworkBindCallback() {
                    @Override
                    public void onBind(InetSocketAddress address) {
                        guard.lockRead();

                        try {
                            if (state.get() == INITIALIZING) {
                                // Continue initialization on the system thread.
                                runOnSysThread(() ->
                                    doInitializeNode(address, localNodeId)
                                );
                            } else {
                                if (DEBUG) {
                                    log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                                }
                            }
                        } finally {
                            guard.unlockRead();
                        }
                    }

                    @Override
                    public NetworkServerFailure.Resolution onFailure(NetworkServerFailure failure) {
                        InetSocketAddress address = failure.lastTriedAddress();

                        String msg = "Failed to start network service [address=" + address + ", reason=" + failure.cause() + ']';

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

    private void doInitializeNode(InetSocketAddress nodeAddress, ClusterNodeId localNodeId) {
        guard.lockWrite();

        try {
            // Make sure that we are still initializing with the same node identifier.
            // Need to perform this check in order to stop early in case of concurrent leave/termination events.
            if (isInitializingFor(localNodeId)) {
                // Initialize node info.
                ClusterAddress address = new ClusterAddress(nodeAddress, localNodeId);

                DefaultClusterNode localNode = new DefaultClusterNodeBuilder()
                    .withAddress(address)
                    .withName(nodeName)
                    .withLocalNode(true)
                    .withJoinOrder(DefaultClusterNode.NON_JOINED_ORDER)
                    .withRoles(nodeRoles)
                    .withProperties(nodeProps)
                    .withServices(services.getServicesInfo())
                    .withSysInfo(DefaultClusterNodeRuntime.getLocalInfo())
                    .createNode();

                node = localNode;

                // Prepare initial (empty) topology.
                topology = DefaultClusterTopology.empty();

                if (log.isInfoEnabled()) {
                    log.info("Initialized local node info [node={}]", localNode.toDetailedString());
                }

                // Initialize services.
                InitializationContext ctx = createInitContext(localNode);

                if (log.isInfoEnabled()) {
                    log.info("Initializing services...");
                }

                services.preInitialize(ctx);

                services.initialize(ctx);

                services.postInitialize(ctx);

                // Register to JMX (optional).
                JmxService jmx = services.findService(JmxService.class);

                if (jmx != null) {
                    jmx.register(this);
                }

                // Start plugins.
                plugins.start(this);

                // Pre-check state since plugin could initiate leave/termination procedure.
                if (isInitializingFor(localNodeId)) {
                    // Update state and notify listeners/future.
                    state.set(INITIALIZED);

                    initFuture.complete(this);

                    notifyOnLifecycleChange();
                }

                if (log.isInfoEnabled()) {
                    log.info("Done initializing services.");
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

    private ClusterContext createClusterContext(DefaultClusterNode localNode) {
        assert localNode != null : "Local node is null.";
        assert guard.isWriteLocked() : "Thread must hold a write.";

        JoinFuture localJoinFuture = joinFuture;

        return new ClusterContext() {
            @Override
            public CompletableFuture<Boolean> onStartJoining() {
                CompletableFuture<Boolean> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    boolean changed = guard.withWriteLock(() -> {
                        if (state.compareAndSet(INITIALIZED, JOINING)) {
                            notifyOnLifecycleChange();

                            return true;
                        } else {
                            return false;
                        }
                    });

                    future.complete(changed);
                });

                return future;
            }

            @Override
            public CompletableFuture<ClusterJoinEvent> onJoin(int joinOrder, Set<ClusterNode> nodes) {
                CompletableFuture<ClusterJoinEvent> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    guard.withWriteLock(() -> {
                        if (state.compareAndSet(JOINING, UP)) {
                            localNode.setJoinOrder(joinOrder);

                            DefaultClusterTopology newTopology = topology.update(nodes);

                            if (DEBUG) {
                                log.debug("Updated local topology [topology={}]", newTopology);
                            }

                            topology = newTopology;

                            if (log.isInfoEnabled()) {
                                int size = newTopology.size();
                                long ver = newTopology.version();
                                String nodesStr = toAddressesString(newTopology);

                                log.info("Joined cluster "
                                    + "[size={}, join-order={}, topology-ver={}, topology={}]", size, joinOrder, ver, nodesStr);
                            }

                            ClusterJoinEvent join = new ClusterJoinEvent(newTopology, hekate());

                            clusterEvents.fireAsync(join).thenRun(() -> {
                                // Notify join future only after the initial join event has been processed by all listeners.
                                runOnSysThread(() ->
                                    localJoinFuture.complete(hekate())
                                );
                            });

                            notifyOnLifecycleChange();

                            future.complete(join);
                        } else {
                            future.complete(null);
                        }
                    });
                });

                return future;
            }

            @Override
            public CompletableFuture<ClusterChangeEvent> onTopologyChange(Set<ClusterNode> nodes) {
                CompletableFuture<ClusterChangeEvent> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    guard.withWriteLock(() -> {
                        if (clusterEvents.isJoinEventFired()) {
                            DefaultClusterTopology lastTopology = topology;

                            DefaultClusterTopology newTopology = lastTopology.updateIfModified(nodes);

                            if (newTopology.version() == lastTopology.version()) {
                                future.complete(null);
                            } else {
                                topology = newTopology;

                                if (DEBUG) {
                                    log.debug("Updated local topology [topology={}]", newTopology);
                                }

                                Set<ClusterNode> oldNodes = lastTopology.nodeSet();
                                Set<ClusterNode> newNodes = newTopology.nodeSet();

                                List<ClusterNode> removed = getDiff(oldNodes, newNodes);
                                List<ClusterNode> added = getDiff(newNodes, oldNodes);

                                if (log.isInfoEnabled()) {
                                    int size = topology.size();
                                    long version = topology.version();
                                    String addresses = toAddressesString(topology);

                                    log.info("Updated cluster topology [size={}, added={}, removed={}, topology-version={}, topology={}]",
                                        size, added, removed, version, addresses);
                                }

                                ClusterChangeEvent event = new ClusterChangeEvent(newTopology, added, removed, hekate());

                                clusterEvents.fireAsync(event);

                                future.complete(event);
                            }
                        } else {
                            future.complete(null);
                        }
                    });
                });

                return future;
            }

            @Override
            public void onLeave() {
                if (log.isInfoEnabled()) {
                    log.info("Done leaving cluster.");
                }

                terminateAsync();
            }

            @Override
            public ClusterTopology topology() {
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

    private InitializationContext createInitContext(DefaultClusterNode localNode) {
        assert localNode != null : "Local node is null.";
        assert guard.isWriteLocked() : "Thread must hold a write.";

        ClusterContext clusterCtx = createClusterContext(localNode);

        return new InitializationContext() {
            @Override
            public String clusterName() {
                return clusterName;
            }

            @Override
            public State state() {
                return state.get();
            }

            @Override
            public ClusterContext cluster() {
                return clusterCtx;
            }

            @Override
            public ClusterNode localNode() {
                return localNode;
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
            public String toString() {
                return ToString.format(InitializationContext.class, this);
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
            if (state.compareAndSet(INITIALIZING, TERMINATING)
                || state.compareAndSet(INITIALIZED, TERMINATING)
                || state.compareAndSet(JOINING, TERMINATING)
                || state.compareAndSet(UP, TERMINATING)
                || state.compareAndSet(LEAVING, TERMINATING)) {
                if (DEBUG) {
                    log.debug("Scheduling task for asynchronous termination [rejoin={}]", rejoin);
                }

                TerminateFuture future = new TerminateFuture();

                terminateFutureRef.set(future);

                if (rejoin) {
                    // Enable rejoin after termination.
                    rejoining.compareAndSet(null, true);
                }

                notifyOnLifecycleChange();

                runOnSysThread(() ->
                    doTerminate(reason, future, cause)
                );

                return future.fork();
            } else if (state.get() == TERMINATING) {
                if (DEBUG) {
                    log.debug("Skipped termination request processing since service is already in {} state.", TERMINATING);
                }

                if (!rejoin) {
                    // Prevent concurrent termination process from rejoining.
                    rejoining.set(false);
                }

                return terminateFutureRef.get().fork();
            } else {
                if (DEBUG) {
                    log.debug("Skipped termination request processing since service is already in {} state.", DOWN);
                }

                TerminateFuture future = new TerminateFuture();

                future.complete(this);

                return future;
            }
        });
    }

    private void doLeave() {
        if (state.get() == LEAVING) {
            clusterEvents.ensureLeaveEventFired(ClusterLeaveReason.LEAVE, topology).thenRun(() ->
                runOnSysThread(this::preTerminateServices)
            );
        }
    }

    private void preTerminateServices() {
        if (!preTerminated) {
            preTerminated = true;

            services.preTerminate();

            plugins.stop();
        }
    }

    private void doTerminate(ClusterLeaveReason reason, TerminateFuture future, Throwable cause) {
        assert future != null : "Termination future is null.";
        assert reason != null : "Reason is null";
        assert state.get() == TERMINATING : "Unexpected service state: " + state;

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
            log.error("Got an unexpected error while awaiting for cluster leave event processing.", e);
        }

        preTerminateServices();

        services.terminate();

        services.postTerminate();

        clusterEvents.stop();

        InitializationFuture notifyInit = null;
        JoinFuture notifyJoin = null;
        LeaveFuture notifyLeave = null;

        guard.lockWrite();

        try {
            state.set(DOWN);

            // Check if we should rejoin after termination.
            boolean rejoin = rejoining.compareAndSet(true, null);

            if (!rejoin) {
                // Always clear this flag in case of non-rejoining termination.
                // Otherwise terminations of subsequent explicit joins will act as rejoin.
                rejoining.set(null);
            }

            // Important to use shutdown NOW in order to prevent stale tasks execution,
            // anyway such tasks were expected to be executed in the context of a node that is already terminated.
            sysWorker.shutdownNow();

            nodeId = null;
            sysWorker = null;
            preTerminated = false;

            terminateFutureRef.set(null);

            // Init/Join/Leave futures must be notified only if this is not a rejoin.
            // Otherwise they can be prematurely notified if rejoining happens before node is UP.
            if (!rejoin) {
                if (!initFuture.isDone()) {
                    notifyInit = initFuture;
                }

                if (!joinFuture.isDone()) {
                    notifyJoin = joinFuture;
                }

                if (!leaveFuture.isDone()) {
                    notifyLeave = leaveFuture;
                }
            }

            if (notifyInit != null || initFuture.isDone()) {
                initFuture = new InitializationFuture();
            }

            if (notifyJoin != null || joinFuture.isDone()) {
                joinFuture = new JoinFuture();
            }

            if (notifyLeave != null || leaveFuture.isDone()) {
                leaveFuture = new LeaveFuture();
            }

            if (log.isInfoEnabled()) {
                log.info("Terminated.");
            }

            notifyOnLifecycleChange();

            // Run rejoin process (if not already requested by a lifecycle listener).
            if (rejoin && state.get() == DOWN) {
                joinAsync();
            }
        } finally {
            guard.unlockWrite();
        }

        if (notifyInit != null) {
            if (DEBUG) {
                log.debug("Notifying initialization future.");
            }

            if (cause == null) {
                notifyInit.complete(this);
            } else {
                notifyInit.completeExceptionally(cause);
            }
        }

        if (notifyJoin != null) {
            if (DEBUG) {
                log.debug("Notifying join future.");
            }

            if (cause == null) {
                notifyJoin.complete(this);
            } else {
                notifyJoin.completeExceptionally(cause);
            }
        }

        if (notifyLeave != null) {
            if (DEBUG) {
                log.debug("Notifying leave future.");
            }

            notifyLeave.complete(this);
        }

        future.complete(this);
    }

    private boolean isInitializingFor(ClusterNodeId localNodeId) {
        return state.get() == INITIALIZING && localNodeId.equals(nodeId);
    }

    private void notifyOnLifecycleChange() {
        for (LifecycleListener listener : listeners) {
            try {
                listener.onStateChanged(this);
            } catch (RuntimeException | Error e) {
                log.error("Failed to notify listener on state change [state={}, listener={}]", state(), listener, e);
            }
        }
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

    private ServiceManager createServiceManager(CodecFactory<Object> codec, List<ServiceFactory<? extends Service>> services) {
        // Wrap codec factory.
        CodecFactory<Object> defaultCodec = HekateCodecHelper.wrap(codec, this);

        // Prepare built-in services.
        List<Service> builtIn = singletonList(new DefaultCodecService(defaultCodec));

        // Prepare core services.
        List<Class<? extends Service>> core = new ArrayList<>();

        core.add(ResourceService.class);
        core.add(NetworkService.class);
        core.add(ClusterService.class);
        core.add(MessagingService.class);
        core.add(RpcService.class);
        core.add(LocalMetricsService.class);
        core.add(ClusterMetricsService.class);
        core.add(LockService.class);
        core.add(ElectionService.class);
        core.add(CoordinationService.class);

        // Prepare custom services.
        List<ServiceFactory<? extends Service>> factories = new ArrayList<>();

        // Filter out null values.
        nullSafe(services).forEach(factories::add);

        return new ServiceManager(nodeName, clusterName, this, builtIn, core, factories);
    }

    private List<ClusterNode> getDiff(Set<ClusterNode> oldNodes, Set<ClusterNode> newNodes) {
        List<ClusterNode> removed = null;

        for (ClusterNode oldNode : oldNodes) {
            if (!newNodes.contains(oldNode)) {
                if (removed == null) {
                    removed = new ArrayList<>(oldNodes.size());
                }

                removed.add(oldNode);
            }
        }

        return removed != null ? unmodifiableList(removed) : emptyList();
    }

    private String toAddressesString(ClusterTopology topology) {
        StringBuilder buf = new StringBuilder();

        topology.nodes().forEach(n -> {
            if (buf.length() > 0) {
                buf.append(", ");
            }

            if (!n.name().isEmpty()) {
                buf.append(n.name()).append('#');
            }

            buf.append(n.address());
        });

        return buf.toString();
    }

    @Override
    public String toString() {
        return Hekate.class.getSimpleName() + "[state=" + state + ", node=" + node + ']';
    }
}
