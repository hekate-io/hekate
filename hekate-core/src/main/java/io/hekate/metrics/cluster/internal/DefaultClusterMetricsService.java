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

package io.hekate.metrics.cluster.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterView;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.messaging.Message;
import io.hekate.messaging.MessagingChannel;
import io.hekate.messaging.MessagingChannelConfig;
import io.hekate.messaging.MessagingConfigProvider;
import io.hekate.messaging.MessagingService;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.MetricValue;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.cluster.ClusterMetricsServiceFactory;
import io.hekate.metrics.cluster.ClusterNodeMetrics;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateRequest;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateResponse;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.util.StateGuard;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;

public class DefaultClusterMetricsService implements ClusterMetricsService, DependentService, InitializingService, TerminatingService,
    MessagingConfigProvider {
    private static class Replica {
        private final ClusterNode node;

        private long version;

        private Map<String, MetricValue> metrics;

        private volatile Optional<ClusterNodeMetrics> publicMetrics = Optional.empty();

        public Replica(ClusterNode node) {
            this.node = node;
        }

        public ClusterNode node() {
            return node;
        }

        public long version() {
            return version;
        }

        public Map<String, MetricValue> metrics() {
            return metrics;
        }

        public void updateMetrics(long version, Map<String, MetricValue> metrics) {
            this.version = version;
            this.metrics = metrics;

            publicMetrics = Optional.of(new DefaultClusterNodeMetrics(node(), unmodifiableMap(new HashMap<>(metrics))));
        }

        public Optional<ClusterNodeMetrics> publicMetrics() {
            return publicMetrics;
        }
    }

    private static class ReplicationTarget {
        private final ClusterNodeId to;

        private final MessagingChannel<MetricsProtocol> channel;

        private final ClusterNodeId localNode;

        private final Map<ClusterNodeId, Long> sentVersions = new HashMap<>();

        public ReplicationTarget(ClusterNodeId to, MessagingChannel<MetricsProtocol> channel, ClusterNodeId localNode) {
            this.to = to;
            this.channel = channel;
            this.localNode = localNode;
        }

        public ClusterNodeId to() {
            return to;
        }

        public void send(Collection<Replica> replicas) {
            List<MetricsUpdate> updates = new ArrayList<>(replicas.size());

            long targetVer = -1;

            synchronized (sentVersions) {
                for (Replica replica : replicas) {
                    if (replica.node().id().equals(to)) {
                        targetVer = replica.version();

                        // Do not send metrics of the target node.
                        continue;
                    }

                    synchronized (replica) {
                        if (replica.metrics() != null && !replica.metrics().isEmpty()) {
                            ClusterNodeId nodeId = replica.node().id();

                            Long prevVer = sentVersions.get(nodeId);

                            long newVer = replica.version();

                            if (prevVer == null || prevVer < newVer) {
                                sentVersions.put(nodeId, newVer);

                                MetricsUpdate update = newUpdate(replica);

                                updates.add(update);
                            }
                        }
                    }
                }
            }

            if (!updates.isEmpty()) {
                if (DEBUG) {
                    log.debug("Sending metrics update [to={}, updates={}]", to, updates);
                }

                UpdateRequest update = new UpdateRequest(localNode, targetVer, updates);

                channel.send(update);
            }
        }

        public void update(Replica replica) {
            synchronized (sentVersions) {
                synchronized (replica) {
                    ClusterNodeId nodeId = replica.node().id();

                    Long oldVer = sentVersions.get(nodeId);

                    if (oldVer == null || oldVer < replica.version()) {
                        sentVersions.put(nodeId, replica.version());
                    }
                }
            }
        }
    }

    private static final String CHANNEL_NAME = "hekate.metrics";

    private static final Logger log = LoggerFactory.getLogger(DefaultClusterMetricsService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final int IDLE_TIMEOUT_MULTIPLY = 3;

    private static final ClusterNodeFilter METRICS_SUPPORT_FILTER = n -> n.hasService(ClusterMetricsService.class);

    private final boolean enabled;

    private final long replicationInterval;

    private final MetricFilter filter;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(ClusterService.class);

    @ToStringIgnore
    private final Map<ClusterNodeId, Replica> replicas = new HashMap<>();

    @ToStringIgnore
    private final AtomicLong localVerSeq = new AtomicLong();

    @ToStringIgnore
    private LocalMetricsService localMetrics;

    @ToStringIgnore
    private MessagingService messaging;

    @ToStringIgnore
    private MessagingChannel<MetricsProtocol> channel;

    @ToStringIgnore
    private ReplicationTarget next;

    @ToStringIgnore
    private ClusterView cluster;

    @ToStringIgnore
    private ScheduledExecutorService worker;

    @ToStringIgnore
    private ClusterNodeId localNode;

    public DefaultClusterMetricsService(ClusterMetricsServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(ClusterMetricsServiceFactory.class);

        check.positive(factory.getReplicationInterval(), "replication interval");

        enabled = factory.isEnabled();
        replicationInterval = factory.getReplicationInterval();
        filter = factory.getReplicationFilter();
    }

    @Override
    public void resolve(DependencyContext ctx) {
        localMetrics = ctx.require(LocalMetricsService.class);
        messaging = ctx.require(MessagingService.class);
        cluster = ctx.require(ClusterService.class).filter(METRICS_SUPPORT_FILTER);
    }

    @Override
    public Collection<MessagingChannelConfig<?>> configureMessaging() {
        if (enabled) {
            return Collections.singleton(
                MessagingChannelConfig.of(MetricsProtocol.class)
                    .withName(CHANNEL_NAME)
                    .withLogCategory(getClass().getName())
                    .withMessageCodec(MetricsProtocolCodec::new)
                    .withIdleSocketTimeout(replicationInterval * IDLE_TIMEOUT_MULTIPLY)
                    .withClusterFilter(METRICS_SUPPORT_FILTER)
                    .withWorkerThreads(1)
                    .withReceiver(this::handleMessage)
            );
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        if (enabled) {
            guard.lockWrite();

            try {
                guard.becomeInitialized();

                if (DEBUG) {
                    log.debug("Initializing...");
                }

                channel = messaging.channel(CHANNEL_NAME, MetricsProtocol.class);

                localNode = ctx.localNode().id();

                cluster.addListener(event ->
                    updateTopology(event.topology().nodes())
                );

                localMetrics.addListener(event -> {
                    try {
                        updateLocalMetrics(event.allMetrics());
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while updating local metrics.", e);
                    }
                });

                worker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("ClusterMetrics"));

                worker.scheduleAtFixedRate(() -> {
                    try {
                        publishMetrics();
                    } catch (RuntimeException | Error e) {
                        log.error("Got an unexpected runtime error while publishing metrics.", e);
                    }
                }, replicationInterval, replicationInterval, TimeUnit.MILLISECONDS);

                if (DEBUG) {
                    log.debug("Initialized.");
                }
            } finally {
                guard.unlockWrite();
            }
        }
    }

    @Override
    public void terminate() throws HekateException {
        if (enabled) {
            Waiting waiting = null;

            guard.lockWrite();

            try {
                if (guard.becomeTerminated()) {
                    if (DEBUG) {
                        log.debug("Terminating...");
                    }

                    if (worker != null) {
                        waiting = AsyncUtils.shutdown(worker);

                        worker = null;
                    }

                    replicas.clear();

                    worker = null;
                    next = null;
                    channel = null;
                    localNode = null;
                }
            } finally {
                guard.unlockWrite();
            }

            if (waiting != null) {
                waiting.awaitUninterruptedly();

                if (DEBUG) {
                    log.debug("Terminated.");
                }
            }
        }
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public Optional<ClusterNodeMetrics> of(ClusterNodeId node) {
        ArgAssert.notNull(node, "Node");

        if (enabled) {
            guard.lockRead();

            try {
                Replica replica = replicas.get(node);

                if (replica != null) {
                    return replica.publicMetrics();
                }

                return Optional.empty();
            } finally {
                guard.unlockRead();
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public Optional<ClusterNodeMetrics> of(ClusterNode node) {
        ArgAssert.notNull(node, "Node");

        return of(node.id());
    }

    @Override
    public List<ClusterNodeMetrics> all() {
        if (enabled) {
            guard.lockRead();

            try {
                List<ClusterNodeMetrics> metrics = new ArrayList<>(replicas.size());

                replicas.values().forEach(r ->
                    r.publicMetrics().ifPresent(metrics::add)
                );

                return metrics;
            } finally {
                guard.unlockRead();
            }
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<ClusterNodeMetrics> all(MetricFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        if (enabled) {
            guard.lockRead();

            try {
                List<ClusterNodeMetrics> metrics = new ArrayList<>(replicas.size());

                replicas.values().forEach(r ->
                    r.publicMetrics().ifPresent(node -> {
                        for (Metric metric : node.allMetrics().values()) {
                            if (filter.accept(metric)) {
                                metrics.add(node);

                                break;
                            }
                        }
                    })
                );

                return metrics;
            } finally {
                guard.unlockRead();
            }
        } else {
            return Collections.emptyList();
        }
    }

    private void publishMetrics() {
        guard.lockRead();

        try {
            if (guard.isInitialized() && next != null) {
                next.send(replicas.values());
            }
        } finally {
            guard.unlockRead();
        }
    }

    private void updateLocalMetrics(Map<String, Metric> metrics) {
        guard.lockWrite();

        try {
            if (guard.isInitialized()) {
                doUpdateLocalMetrics(metrics);
            }
        } finally {
            guard.unlockWrite();
        }
    }

    private void doUpdateLocalMetrics(Map<String, Metric> metricsMap) {
        assert guard.isWriteLocked() : "Thread must hold write lock.";

        Collection<Metric> metrics = metricsMap.values();

        Map<String, MetricValue> fixedMetrics = new HashMap<>(metrics.size(), 1.0f);

        metrics.forEach(metric -> {
            if (filter == null || filter.accept(metric)) {
                String name = metric.name();

                fixedMetrics.put(name, new MetricValue(name, metric.value()));
            }
        });

        // Update state of the local replica.
        Replica local = replicas.get(localNode);

        if (local != null) {
            long newVer = localVerSeq.incrementAndGet();

            synchronized (local) {
                local.updateMetrics(newVer, fixedMetrics);
            }
        }
    }

    private void updateTopology(List<ClusterNode> nodes) {
        guard.lockWrite();

        try {
            if (guard.isInitialized()) {
                Set<ClusterNodeId> ids = nodes.stream().map(ClusterNode::id).collect(toSet());

                replicas.keySet().retainAll(ids);

                boolean initial = replicas.isEmpty();

                nodes.stream()
                    .filter(node -> !replicas.containsKey(node.id()))
                    .forEach(node -> replicas.put(node.id(), new Replica(node)));

                if (initial) {
                    doUpdateLocalMetrics(localMetrics.allMetrics());
                }

                NavigableSet<ClusterNodeId> ring = new TreeSet<>(ids);

                ClusterNodeId newNext = ring.tailSet(localNode, false).stream()
                    .findFirst()
                    .orElse(ring.headSet(localNode, false).stream()
                        .findFirst()
                        .orElse(null));

                if (newNext == null) {
                    if (DEBUG && next != null) {
                        log.debug("Stopped metrics replication [to={}]", next.to());
                    }

                    next = null;
                } else if (next == null || !next.to().equals(newNext)) {
                    if (DEBUG) {
                        log.debug("Selected new replication target [node={}, ring={}]", newNext, ring);
                    }

                    MessagingChannel<MetricsProtocol> channel = this.channel.forNode(newNext);

                    next = new ReplicationTarget(newNext, channel, localNode);
                }
            }
        } finally {
            guard.unlockWrite();
        }
    }

    private void handleMessage(Message<MetricsProtocol> msg) {
        guard.lockRead();

        try {
            if (guard.isInitialized() && !replicas.isEmpty()) {
                MetricsProtocol in = msg.get();

                switch (in.type()) {
                    case UPDATE_REQUEST: {
                        UpdateRequest request = (UpdateRequest)in;

                        List<MetricsUpdate> updates = request.updates();

                        List<MetricsUpdate> pushBack = processUpdates(in.from(), updates, true, request.targetVer());

                        if (!pushBack.isEmpty()) {
                            if (DEBUG) {
                                log.debug("Sending push back updates [from={}, metrics={}]", in.from(), pushBack);
                            }

                            UpdateResponse response = new UpdateResponse(localNode, pushBack);

                            msg.endpoint().channel().forNode(in.from()).send(response);
                        }

                        break;
                    }
                    case UPDATE_RESPONSE: {
                        UpdateResponse response = (UpdateResponse)in;

                        processUpdates(in.from(), response.metrics(), false, -1);

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected message type: " + in.type());
                    }
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private List<MetricsUpdate> processUpdates(ClusterNodeId from, List<MetricsUpdate> updates, boolean withPushBack, long localVer) {
        List<MetricsUpdate> pushBack;

        if (withPushBack) {
            pushBack = new ArrayList<>(updates.size());

            Replica local = replicas.get(localNode);

            synchronized (local) {
                if (localVer != local.version()) {
                    pushBack.add(newUpdate(local));
                }
            }
        } else {
            pushBack = null;
        }

        updates.stream()
            .filter(update -> !update.node().equals(localNode))
            .forEach(update -> {
                ClusterNodeId node = update.node();

                Replica replica = replicas.get(node);

                if (replica != null) {
                    synchronized (replica) {
                        long newVer = update.version();

                        if (newVer > replica.version()) {
                            ////////////////////////////////////////////////////////
                            // Remote is later -> Update local metrics.
                            ////////////////////////////////////////////////////////
                            Map<String, MetricValue> remoteMetrics = update.metrics();

                            if (DEBUG) {
                                log.debug("Updating metrics [node={}, metrics={}]", node, remoteMetrics);
                            }

                            replica.updateMetrics(newVer, remoteMetrics);

                            if (next != null && from.equals(next.to())) {
                                next.update(replica);
                            }
                        } else if (newVer < replica.version()) {
                            ////////////////////////////////////////////////////////
                            // Local is later -> Send latest version back to remote (if required).
                            ////////////////////////////////////////////////////////
                            if (pushBack != null) {
                                MetricsUpdate newUpdate = newUpdate(replica);

                                pushBack.add(newUpdate);
                            }
                        }
                    }
                }
            });

        return pushBack;
    }

    private static MetricsUpdate newUpdate(Replica replica) {
        return new MetricsUpdate(replica.node().id(), replica.version(), replica.metrics());
    }

    @Override
    public String toString() {
        return ToString.format(ClusterMetricsService.class, this);
    }
}
