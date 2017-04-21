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
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.internal.DefaultClusterNode;
import io.hekate.cluster.internal.DefaultClusterNodeBuilder;
import io.hekate.cluster.internal.DefaultClusterTopology;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.internal.DefaultCodecService;
import io.hekate.coordinate.CoordinationService;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.HekateFutureException;
import io.hekate.core.HekateVersion;
import io.hekate.core.JoinFuture;
import io.hekate.core.LeaveFuture;
import io.hekate.core.SystemInfo;
import io.hekate.core.TerminateFuture;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
import io.hekate.core.service.ClusterContext;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.hekate.core.service.internal.ServiceManager;
import io.hekate.election.ElectionService;
import io.hekate.lock.LockService;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.network.NetworkService;
import io.hekate.network.internal.NetworkBindCallback;
import io.hekate.network.internal.NetworkServerFailure;
import io.hekate.network.internal.NetworkServiceManager;
import io.hekate.partition.PartitionService;
import io.hekate.task.TaskService;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.Hekate.State.DOWN;
import static io.hekate.core.Hekate.State.INITIALIZING;
import static io.hekate.core.Hekate.State.JOINING;
import static io.hekate.core.Hekate.State.LEAVING;
import static io.hekate.core.Hekate.State.TERMINATING;
import static io.hekate.core.Hekate.State.UP;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toSet;

class HekateNode implements Hekate, Serializable {
    private static class SerializationHandle implements Serializable {
        private static final SerializationHandle INSTANCE = new SerializationHandle();

        private static final long serialVersionUID = 1;

        protected Object readResolve() {
            return HekateCodecHelper.getThreadLocal();
        }
    }

    private static final Logger log = LoggerFactory.getLogger(HekateNode.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String nodeName;

    private final String clusterName;

    private final Set<String> nodeRoles;

    private final Map<String, String> nodeProps;

    private final SystemInfo sysInfo;

    private final NetworkServiceManager net;

    private final PluginManager plugins;

    private final ServiceManager services;

    private final StateGuard guard = new StateGuard(Hekate.class);

    private final List<LifecycleListener> listeners = new CopyOnWriteArrayList<>();

    private final AtomicReference<Boolean> rejoining = new AtomicReference<>();

    private final AtomicReference<State> state = new AtomicReference<>(DOWN);

    private final AtomicReference<TerminateFuture> terminateFutureRef = new AtomicReference<>();

    private final Map<String, Object> attributes = Collections.synchronizedMap(new HashMap<>());

    private final ClusterEventManager clusterEvents;

    private final NetworkService network;

    private final ClusterService cluster;

    private final TaskService tasks;

    private final MessagingService messaging;

    private final LockService locks;

    private final ElectionService election;

    private final CoordinationService coordination;

    private final LocalMetricsService localMetrics;

    private final ClusterMetricsService clusterMetrics;

    private final PartitionService partitions;

    private final CodecService codec;

    private boolean joinEventFired;

    private boolean preTerminated;

    private JoinFuture joinFuture = new JoinFuture();

    private LeaveFuture leaveFuture = new LeaveFuture();

    private ClusterNodeId nodeId;

    private volatile DefaultClusterTopology topology;

    private volatile ScheduledExecutorService sysWorker;

    private volatile DefaultClusterNode node;

    public HekateNode(HekateBootstrap cfg) {
        assert cfg != null : "Bootstrap is null.";

        // Install plugins.
        plugins = new PluginManager(cfg);

        plugins.install();

        // Check configuration.
        ConfigCheck check = ConfigCheck.get(HekateBootstrap.class);

        check.notEmpty(cfg.getClusterName(), "cluster name");
        check.notNull(cfg.getDefaultCodec(), "default codec");
        check.isFalse(cfg.getDefaultCodec().createCodec().isStateful(), "default codec can't be stateful.");

        // Basic properties.
        this.nodeName = cfg.getNodeName() != null ? cfg.getNodeName().trim() : "";
        this.clusterName = cfg.getClusterName().trim();

        // System info.
        sysInfo = DefaultSystemInfo.getLocalInfo();

        // Codec factory.
        CodecFactory<Object> wrappedCodecFactory = HekateCodecHelper.wrap(cfg.getDefaultCodec(), this);

        // Node roles.
        Set<String> roles;

        if (cfg.getNodeRoles() == null) {
            roles = emptySet();
        } else {
            // Filter out nulls and trim non-null values.
            roles = unmodifiableSet(Utils.nullSafe(cfg.getNodeRoles()).map(String::trim).collect(toSet()));
        }

        // Node properties.
        Map<String, String> props = new HashMap<>();

        if (cfg.getNodeProperties() != null) {
            cfg.getNodeProperties().forEach((k, v) -> {
                // Trim non-null property keys and values.
                String key = k == null ? null : k.trim();
                String value = v == null ? null : v.trim();

                props.put(key, value);
            });
        }

        // Node properties from providers.
        if (cfg.getNodePropertyProviders() != null) {
            Utils.nullSafe(cfg.getNodePropertyProviders()).forEach(provider -> {
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

        // Cluster event manager.
        this.clusterEvents = new ClusterEventManager();

        // Prepare built-in services.
        List<Class<? extends Service>> requiredServices = new ArrayList<>();

        requiredServices.add(NetworkService.class);
        requiredServices.add(ClusterService.class);
        requiredServices.add(MessagingService.class);
        requiredServices.add(TaskService.class);
        requiredServices.add(LocalMetricsService.class);
        requiredServices.add(ClusterMetricsService.class);
        requiredServices.add(LockService.class);
        requiredServices.add(ElectionService.class);
        requiredServices.add(CoordinationService.class);
        requiredServices.add(PartitionService.class);

        // Prepare core services.
        List<Service> coreServices = new ArrayList<>();

        coreServices.add(new DefaultCodecService(wrappedCodecFactory));

        // Prepare custom services.
        List<ServiceFactory<? extends Service>> serviceFactories = new ArrayList<>();

        if (cfg.getServices() != null) {
            // Filter out null values.
            Utils.nullSafe(cfg.getServices()).forEach(serviceFactories::add);
        }

        // Services manager.
        services = new ServiceManager(coreServices, requiredServices, serviceFactories);

        // Instantiate services.
        ConfigurationContext servicesConfigCtx = services.instantiate(roles, props);

        this.nodeRoles = servicesConfigCtx.getNodeRoles();
        this.nodeProps = servicesConfigCtx.getNodeProperties();

        codec = services.findService(CodecService.class);
        cluster = services.findService(ClusterService.class);
        messaging = services.findService(MessagingService.class);
        network = services.findService(NetworkService.class);
        locks = services.findService(LockService.class);
        election = services.findService(ElectionService.class);
        coordination = services.findService(CoordinationService.class);
        partitions = services.findService(PartitionService.class);
        tasks = services.findService(TaskService.class);
        localMetrics = services.findService(LocalMetricsService.class);
        clusterMetrics = services.findService(ClusterMetricsService.class);

        // Get internal service managers.
        net = services.findService(NetworkServiceManager.class);

        check.notNull(net, NetworkServiceManager.class.getName(), "not found");
    }

    @Override
    public JoinFuture joinAsync() {
        return doJoin(false);
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

        guard.lockWrite();

        try {
            if (state.get() == DOWN) {
                if (DEBUG) {
                    log.debug("Skipped leave request since already in {} state.", state);
                }

                // Not joined. Return a future that is immediately completed.
                LeaveFuture future = new LeaveFuture();

                future.complete(this);

                return future;
            } else if (state.get() == INITIALIZING) {
                // Since we are still initializing it is safe to run termination process (and bypass leave protocol).
                terminateAsync();

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
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public Hekate leave() throws InterruptedException, HekateFutureException {
        return leaveAsync().get();
    }

    @Override
    public ClusterService cluster() {
        return cluster;
    }

    @Override
    public TaskService tasks() {
        return tasks;
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
    public PartitionService partitions() {
        return partitions;
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
    public Set<Class<? extends Service>> getServiceTypes() {
        return services.getServiceTypes();
    }

    @Override
    public State getState() {
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
    public SystemInfo getSysInfo() {
        return sysInfo;
    }

    @Override
    public TerminateFuture terminateAsync() {
        return doTerminateAsync(null);
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
    public ClusterNode getLocalNode() {
        ClusterNode node = this.node;

        if (node == null) {
            throw new IllegalStateException(Hekate.class.getSimpleName() + " is not initialized.");
        }

        return node;
    }

    protected Object writeReplace() throws ObjectStreamException {
        return SerializationHandle.INSTANCE;
    }

    private JoinFuture doJoin(boolean rejoin) {
        if (DEBUG) {
            log.debug("Joining...");
        }

        guard.lockWrite();

        try {
            switch (state.get()) {
                case DOWN: {
                    state.set(INITIALIZING);

                    break;
                }
                case INITIALIZING:
                case JOINING:
                case UP: {
                    if (DEBUG) {
                        log.debug("Skipped join request since already in {} state.", state);
                    }

                    return joinFuture.fork();
                }
                case LEAVING:
                case TERMINATING: {
                    // Report an error only if this is not a rejoin.
                    if (rejoin) {
                        if (DEBUG) {
                            log.debug("Skipped rejoin request since already in {} state.", state);
                        }

                        // Safe to return null from rejoin.
                        return null;
                    } else {
                        throw new IllegalStateException(Hekate.class.getSimpleName() + " is in " + state + " state.");
                    }
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
            if (isInitializingForNodeId(localNodeId)) {
                // Schedule asynchronous initialization and join.
                runOnSysThread(() -> selectAddressAndBind(localNodeId));
            }

            return joinFuture.fork();
        } finally {
            guard.unlockWrite();
        }
    }

    private void selectAddressAndBind(ClusterNodeId localNodeId) {
        try {
            guard.lockWrite();

            try {
                // Make sure that we are still initializing with the same node identifier.
                // Need to perform this check in order to stop early in case of concurrent leave/termination events.
                if (isInitializingForNodeId(localNodeId)) {
                    if (log.isInfoEnabled()) {
                        log.info("Initializing {}.", HekateVersion.getInfo());
                    }

                    if (log.isInfoEnabled()) {
                        log.info("Joining cluster [cluster-name={}, node-name={}]", clusterName, nodeName);
                    }

                    // Bind network service.
                    net.bind(new NetworkBindCallback() {
                        @Override
                        public void onBind(InetSocketAddress address) {
                            guard.lockRead();

                            try {
                                if (state.get() == INITIALIZING) {
                                    runOnSysThread(() -> initialize(address, localNodeId));
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
                            InetSocketAddress address = failure.getLastTriedAddress();

                            String msg = "Failed to start network service [address=" + address + ", reason=" + failure.getCause() + ']';

                            doTerminateAsync(new HekateException(msg, failure.getCause()));

                            return failure.fail();
                        }
                    });
                } else {
                    if (DEBUG) {
                        log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                    }
                }
            } finally {
                guard.unlockWrite();
            }
        } catch (HekateException | RuntimeException | Error e) {
            doTerminateAsync(e);
        }
    }

    private void initialize(InetSocketAddress nodeAddress, ClusterNodeId localNodeId) {

        try {
            InitializationContext ctx = null;

            guard.lockWrite();

            try {
                // Make sure that we are still initializing with the same node identifier.
                // Need to perform this check in order to stop early in case of concurrent leave/termination events.
                if (isInitializingForNodeId(localNodeId)) {
                    // Initialize node info.
                    ClusterAddress address = new ClusterAddress(nodeAddress, nodeId);

                    DefaultClusterNode localNode = new DefaultClusterNodeBuilder()
                        .withAddress(address)
                        .withName(nodeName)
                        .withLocalNode(true)
                        .withJoinOrder(DefaultClusterNode.NON_JOINED_ORDER)
                        .withRoles(nodeRoles)
                        .withProperties(nodeProps)
                        .withServices(services.getServicesInfo())
                        .withSysInfo(sysInfo)
                        .createNode();

                    node = localNode;

                    // Prepare initial (empty) topology.
                    topology = new DefaultClusterTopology(0, emptySet());

                    if (log.isInfoEnabled()) {
                        log.info("Initialized local node info [node={}]", localNode.toDetailedString());
                    }

                    // Initialize services and plugins.
                    ctx = newInitializationContext(localNode);
                } else {
                    if (DEBUG) {
                        log.debug("Stopped initialization sequence due to a concurrent leave/terminate event.");
                    }
                }
            } finally {
                guard.unlockWrite();
            }

            // Initialize services in unlocked context since we don't want to block concurrent leave/terminate requests.
            if (ctx != null) {
                if (log.isInfoEnabled()) {
                    log.info("Initializing services...");
                }

                services.preInitialize(ctx);

                services.initialize(ctx);

                plugins.start(this);

                services.postInitialize(ctx);

                if (log.isInfoEnabled()) {
                    log.info("Done initializing services.");
                }
            }
        } catch (HekateException | RuntimeException | Error e) {
            doTerminateAsync(e);
        }
    }

    private ClusterContext newClusterContext(DefaultClusterNode localNode) {
        JoinFuture localJoinFuture = joinFuture;

        return new ClusterContext() {
            @Override
            public CompletableFuture<Boolean> onStartJoining() {
                CompletableFuture<Boolean> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    boolean changed;

                    guard.lockWrite();

                    try {
                        changed = state.compareAndSet(INITIALIZING, JOINING);

                        if (changed) {
                            notifyOnLifecycleChange();
                        }
                    } finally {
                        guard.unlockWrite();
                    }

                    future.complete(changed);
                });

                return future;
            }

            @Override
            public CompletableFuture<ClusterJoinEvent> onJoin(int joinOrder, Set<ClusterNode> nodes) {
                CompletableFuture<ClusterJoinEvent> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    guard.lockWrite();

                    try {
                        if (state.compareAndSet(JOINING, UP)) {
                            localNode.setJoinOrder(joinOrder);

                            DefaultClusterTopology newTopology = topology.update(nodes);

                            if (DEBUG) {
                                log.debug("Updated local topology [topology={}]", newTopology);
                            }

                            topology = newTopology;

                            if (log.isInfoEnabled()) {
                                int size = newTopology.size();
                                long ver = newTopology.getVersion();
                                String nodesStr = toAddressesString(newTopology);

                                log.info("Joined cluster "
                                    + "[size={}, join-order={}, topology-ver={}, topology={}]", size, joinOrder, ver, nodesStr);
                            }

                            joinEventFired = true;

                            ClusterJoinEvent join = new ClusterJoinEvent(newTopology);

                            clusterEvents.fireAsync(join, () -> {
                                    // Notify join future only after the initial join event has been processed by all listeners.
                                    runOnSysThread(() -> localJoinFuture.complete(HekateNode.this));
                                }
                            );

                            notifyOnLifecycleChange();

                            future.complete(join);
                        } else {
                            future.complete(null);
                        }
                    } finally {
                        guard.unlockWrite();
                    }
                });

                return future;
            }

            @Override
            public CompletableFuture<ClusterChangeEvent> onTopologyChange(Set<ClusterNode> nodes) {
                CompletableFuture<ClusterChangeEvent> future = new CompletableFuture<>();

                runOnSysThread(() -> {
                    guard.lockWrite();

                    try {
                        if (joinEventFired) {
                            DefaultClusterTopology lastTopology = topology;

                            DefaultClusterTopology newTopology = lastTopology.updateIfModified(nodes);

                            if (newTopology.getVersion() == lastTopology.getVersion()) {
                                future.complete(null);
                            } else {
                                topology = newTopology;

                                if (DEBUG) {
                                    log.debug("Updated local topology [topology={}]", newTopology);
                                }

                                Set<ClusterNode> oldNodes = lastTopology.getNodes();
                                Set<ClusterNode> newNodes = newTopology.getNodes();

                                Set<ClusterNode> removed = getDiff(oldNodes, newNodes);
                                Set<ClusterNode> added = getDiff(newNodes, oldNodes);

                                if (log.isInfoEnabled()) {
                                    int size = topology.size();
                                    long version = topology.getVersion();
                                    String addresses = toAddressesString(topology);

                                    log.info("Updated cluster topology [size={}, added={}, removed={}, topology-version={}, topology={}]",
                                        size, added, removed, version, addresses);
                                }

                                ClusterChangeEvent event = new ClusterChangeEvent(newTopology, added, removed);

                                clusterEvents.fireAsync(event);

                                future.complete(event);
                            }
                        } else {
                            future.complete(null);
                        }
                    } finally {
                        guard.unlockWrite();
                    }
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
            public ClusterTopology getTopology() {
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

    private InitializationContext newInitializationContext(DefaultClusterNode localNode) {
        ClusterContext clusterCtx = newClusterContext(localNode);

        return new InitializationContext() {
            @Override
            public String getClusterName() {
                return clusterName;
            }

            @Override
            public State getState() {
                return state.get();
            }

            @Override
            public ClusterContext getCluster() {
                return clusterCtx;
            }

            @Override
            public ClusterNode getNode() {
                return localNode;
            }

            @Override
            public Hekate getHekate() {
                return HekateNode.this;
            }

            @Override
            public void rejoin() {
                doTerminateAsync(true, null);
            }

            @Override
            public void terminate() {
                terminateAsync();
            }

            @Override
            public void terminate(Throwable e) {
                doTerminateAsync(e);
            }

            @Override
            public String toString() {
                return ToString.format(InitializationContext.class, this);
            }
        };
    }

    private TerminateFuture doTerminateAsync(Throwable cause) {
        return doTerminateAsync(false, cause);
    }

    private TerminateFuture doTerminateAsync(boolean rejoin, Throwable cause) {
        guard.lockWrite();

        try {
            if (state.compareAndSet(INITIALIZING, TERMINATING)
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

                runOnSysThread(() -> doTerminate(cause, future));

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
        } finally {
            guard.unlockWrite();
        }
    }

    private void doLeave() {
        if (state.get() == LEAVING) {
            ensureLeaveEventFired(() -> runOnSysThread(this::preTerminateServices));
        }
    }

    private void ensureLeaveEventFired(Runnable doAfterFired) {
        // If join event was fired then we need to fire leave event too.
        if (joinEventFired) {
            joinEventFired = false;

            ClusterLeaveEvent event = new ClusterLeaveEvent(topology, emptySet(), emptySet());

            clusterEvents.fireAsync(event, doAfterFired);
        } else {
            doAfterFired.run();
        }
    }

    private void preTerminateServices() {
        if (!preTerminated) {
            preTerminated = true;

            services.preTerminate();

            plugins.stop();
        }
    }

    private void doTerminate(Throwable cause, TerminateFuture localTerminateFuture) {
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

        CountDownLatch latch = new CountDownLatch(1);

        ensureLeaveEventFired(latch::countDown);

        ((Waiting)latch::await).awaitUninterruptedly();

        preTerminateServices();

        services.terminate();

        services.postTerminate();

        clusterEvents.stop();

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

            // Join/Leave futures must be notified only if this is not a rejoin.
            // Otherwise they can be prematurely notified if rejoining happens before node is UP.
            if (!rejoin) {
                if (!joinFuture.isDone()) {
                    notifyJoin = joinFuture;
                }

                if (!leaveFuture.isDone()) {
                    notifyLeave = leaveFuture;
                }
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
                doJoin(true);
            }
        } finally {
            guard.unlockWrite();
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

        localTerminateFuture.complete(this);
    }

    private boolean isInitializingForNodeId(ClusterNodeId localNodeId) {
        return state.get() == INITIALIZING && localNodeId.equals(nodeId);
    }

    private void notifyOnLifecycleChange() {
        for (LifecycleListener listener : listeners) {
            try {
                listener.onStateChanged(this);
            } catch (RuntimeException | Error e) {
                log.error("Failed to notify listener on state change [state={}, listener={}]", getState(), listener, e);
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

    private Set<ClusterNode> getDiff(Set<ClusterNode> oldNodes, Set<ClusterNode> newNodes) {
        Set<ClusterNode> removed = null;

        for (ClusterNode oldNode : oldNodes) {
            if (!newNodes.contains(oldNode)) {
                if (removed == null) {
                    removed = new HashSet<>(oldNodes.size(), 1.0f);
                }

                removed.add(oldNode);
            }
        }

        return removed != null ? unmodifiableSet(removed) : emptySet();
    }

    private String toAddressesString(ClusterTopology topology) {
        StringBuilder buf = new StringBuilder();

        topology.getSorted().forEach(n -> {
            if (buf.length() > 0) {
                buf.append(", ");
            }

            if (!n.getName().isEmpty()) {
                buf.append(n.getName()).append('#');
            }

            buf.append(n.getAddress());
        });

        return buf.toString();
    }

    @Override
    public String toString() {
        return Hekate.class.getSimpleName() + "[state=" + state + ", node=" + node + ']';
    }
}
