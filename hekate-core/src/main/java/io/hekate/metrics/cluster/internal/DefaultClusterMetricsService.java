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

package io.hekate.metrics.cluster.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterUuid;
import io.hekate.cluster.ClusterView;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
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
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.cluster.ClusterMetricsServiceFactory;
import io.hekate.metrics.cluster.ClusterNodeMetrics;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateRequest;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateResponse;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.internal.StaticMetric;
import io.hekate.util.StateGuard;
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

        private Map<String, StaticMetric> metrics;

        private volatile Optional<ClusterNodeMetrics> publicMetrics = Optional.empty();

        public Replica(ClusterNode node) {
            this.node = node;
        }

        public ClusterNode getNode() {
            return node;
        }

        public long getVersion() {
            return version;
        }

        public Map<String, StaticMetric> getMetrics() {
            return metrics;
        }

        public void updateMetrics(long version, Map<String, StaticMetric> metrics) {
            this.version = version;
            this.metrics = metrics;

            publicMetrics = Optional.of(new DefaultClusterNodeMetrics(getNode(), unmodifiableMap(new HashMap<>(metrics))));
        }

        public Optional<ClusterNodeMetrics> getPublicMetrics() {
            return publicMetrics;
        }
    }

    private static class ReplicationTarget {
        private final ClusterUuid to;

        private final MessagingChannel<MetricsProtocol> channel;

        private final ClusterUuid localNode;

        private final Map<ClusterUuid, Long> sentVersions = new HashMap<>();

        public ReplicationTarget(ClusterUuid to, MessagingChannel<MetricsProtocol> channel, ClusterUuid localNode) {
            this.to = to;
            this.channel = channel;
            this.localNode = localNode;
        }

        public ClusterUuid getTo() {
            return to;
        }

        public void send(Collection<Replica> replicas) {
            List<MetricsUpdate> updates = new ArrayList<>(replicas.size());

            long targetVer = -1;

            synchronized (sentVersions) {
                for (Replica replica : replicas) {
                    if (replica.getNode().getId().equals(to)) {
                        targetVer = replica.getVersion();

                        // Do not send metrics of the target node.
                        continue;
                    }

                    synchronized (replica) {
                        if (replica.getMetrics() != null && !replica.getMetrics().isEmpty()) {
                            ClusterUuid nodeId = replica.getNode().getId();

                            Long prevVer = sentVersions.get(nodeId);

                            long newVer = replica.getVersion();

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
                    ClusterUuid nodeId = replica.getNode().getId();

                    Long oldVer = sentVersions.get(nodeId);

                    if (oldVer == null || oldVer < replica.getVersion()) {
                        sentVersions.put(nodeId, replica.getVersion());
                    }
                }
            }
        }
    }

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
    private final Map<ClusterUuid, Replica> replicas = new HashMap<>();

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
    private ClusterUuid localNode;

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
            MessagingChannelConfig<MetricsProtocol> channelCfg = new MessagingChannelConfig<>();

            channelCfg.setName(MetricsProtocolCodec.PROTOCOL_ID);
            channelCfg.setLogCategory(getClass().getName());
            channelCfg.setMessageCodec(MetricsProtocolCodec::new);
            channelCfg.setIdleTimeout(replicationInterval * IDLE_TIMEOUT_MULTIPLY);
            channelCfg.setReceiver(this::handleMessage);
            channelCfg.setClusterFilter(METRICS_SUPPORT_FILTER);
            channelCfg.setWorkerThreads(1);

            return Collections.singleton(channelCfg);
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

                channel = messaging.channel(MetricsProtocolCodec.PROTOCOL_ID);

                localNode = ctx.getNode().getId();

                cluster.addListener(event ->
                    updateTopology(event.getTopology().getNodes())
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
                        waiting = Utils.shutdown(worker);

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
    public Optional<ClusterNodeMetrics> of(ClusterUuid node) {
        ArgAssert.notNull(node, "Node");

        if (enabled) {
            guard.lockRead();

            try {
                Replica replica = replicas.get(node);

                if (replica != null) {
                    return replica.getPublicMetrics();
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

        return of(node.getId());
    }

    @Override
    public List<ClusterNodeMetrics> all() {
        if (enabled) {
            guard.lockRead();

            try {
                List<ClusterNodeMetrics> metrics = new ArrayList<>(replicas.size());

                replicas.values().forEach(r ->
                    r.getPublicMetrics().ifPresent(metrics::add)
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
                    r.getPublicMetrics().ifPresent(node -> {
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

        Map<String, StaticMetric> fixedMetrics = new HashMap<>(metrics.size(), 1.0f);

        metrics.forEach(metric -> {
            if (filter == null || filter.accept(metric)) {
                String name = metric.getName();

                fixedMetrics.put(name, new StaticMetric(name, metric.getValue()));
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
                Set<ClusterUuid> ids = nodes.stream().map(ClusterNode::getId).collect(toSet());

                replicas.keySet().retainAll(ids);

                boolean initial = replicas.isEmpty();

                nodes.stream()
                    .filter(node -> !replicas.containsKey(node.getId()))
                    .forEach(node -> replicas.put(node.getId(), new Replica(node)));

                if (initial) {
                    doUpdateLocalMetrics(localMetrics.allMetrics());
                }

                NavigableSet<ClusterUuid> ring = new TreeSet<>(ids);

                ClusterUuid newNext = ring.tailSet(localNode, false).stream()
                    .findFirst()
                    .orElse(ring.headSet(localNode, false).stream()
                        .findFirst()
                        .orElse(null));

                if (newNext == null) {
                    if (DEBUG && next != null) {
                        log.debug("Stopped metrics replication [to={}]", next.getTo());
                    }

                    next = null;
                } else if (next == null || !next.getTo().equals(newNext)) {
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

                switch (in.getType()) {
                    case UPDATE_REQUEST: {
                        UpdateRequest request = (UpdateRequest)in;

                        List<MetricsUpdate> updates = request.getUpdates();

                        List<MetricsUpdate> pushBack = processUpdates(in.getFrom(), updates, true, request.getTargetVer());

                        if (!pushBack.isEmpty()) {
                            if (DEBUG) {
                                log.debug("Sending push back updates [from={}, metrics={}]", in.getFrom(), pushBack);
                            }

                            UpdateResponse response = new UpdateResponse(localNode, pushBack);

                            msg.getEndpoint().getChannel().forNode(in.getFrom()).send(response);
                        }

                        break;
                    }
                    case UPDATE_RESPONSE: {
                        UpdateResponse response = (UpdateResponse)in;

                        processUpdates(in.getFrom(), response.getMetrics(), false, -1);

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected message type: " + in.getType());
                    }
                }
            }
        } finally {
            guard.unlockRead();
        }
    }

    private List<MetricsUpdate> processUpdates(ClusterUuid from, List<MetricsUpdate> updates, boolean withPushBack, long localVer) {
        List<MetricsUpdate> pushBack;

        if (withPushBack) {
            pushBack = new ArrayList<>(updates.size());

            Replica local = replicas.get(localNode);

            synchronized (local) {
                if (localVer != local.getVersion()) {
                    pushBack.add(newUpdate(local));
                }
            }
        } else {
            pushBack = null;
        }

        updates.stream()
            .filter(update -> !update.getNode().equals(localNode))
            .forEach(update -> {
                ClusterUuid node = update.getNode();

                Replica replica = replicas.get(node);

                if (replica != null) {
                    synchronized (replica) {
                        long newVer = update.getVersion();

                        if (newVer > replica.getVersion()) {
                            ////////////////////////////////////////////////////////
                            // Remote is later -> Update local metrics.
                            ////////////////////////////////////////////////////////
                            Map<String, StaticMetric> remoteMetrics = update.getMetrics();

                            if (DEBUG) {
                                log.debug("Updating metrics [node={}, metrics={}]", node, remoteMetrics);
                            }

                            replica.updateMetrics(newVer, remoteMetrics);

                            if (next != null && from.equals(next.getTo())) {
                                next.update(replica);
                            }
                        } else if (newVer < replica.getVersion()) {
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
        return new MetricsUpdate(replica.getNode().getId(), replica.getVersion(), replica.getMetrics());
    }

    @Override
    public String toString() {
        return ToString.format(ClusterMetricsService.class, this);
    }
}
