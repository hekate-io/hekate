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

package io.hekate.cluster.health;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Default implementation of {@link FailureDetector} interface.
 *
 * <p>
 * This implementation organizes all nodes into a ring with each node being monitored by a configurable number of other nodes. Monitoring
 * is based on periodic exchange of heartbeat messages with a configurable time interval and a configurable amount of acceptable heartbeat
 * losses.
 * </p>
 *
 * <p>
 * Configuration options of this failure detector are represented by the {@link DefaultFailureDetectorConfig} class with the following
 * properties:
 * </p>
 * <ul>
 * <li>{@link DefaultFailureDetectorConfig#setFailureDetectionQuorum(int) Failure detection quorum} - specifies how many nodes will be
 * monitored by this node</li>
 * <li>{@link DefaultFailureDetectorConfig#setHeartbeatInterval(long) Heartbeat interval} - time interval in milliseconds between attempts
 * of sending a heartbeat request message to the monitored node</li>
 * <li>{@link DefaultFailureDetectorConfig#setHeartbeatLossThreshold(int) Heartbeat loss threshold} - acceptable amount of lost heartbeats
 * before considering the monitored node to be failed. Note that heartbeat is considered to be lost if no response for it was received
 * during the {@link DefaultFailureDetectorConfig#setHeartbeatInterval(long) heartbeat interval}.</li>
 * </ul>
 *
 * @see DefaultFailureDetectorConfig
 * @see ClusterServiceFactory#setFailureDetector(FailureDetector)
 */
public class DefaultFailureDetector implements FailureDetector, JmxSupport<DefaultFailureDetectorJmx> {
    private static class NodeMonitor {
        private final ClusterAddress address;

        private final int hbLossThreshold;

        private final long hbInterval;

        private int lostHeartbeats = -1;

        private boolean connectFailure;

        public NodeMonitor(ClusterAddress address, int hbLossThreshold, long hbInterval) {
            this.address = address;
            this.hbLossThreshold = hbLossThreshold;
            this.hbInterval = hbInterval;
        }

        public synchronized void onHeartbeatSent() {
            lostHeartbeats++;

            if (DEBUG) {
                if (lostHeartbeats > 0 && lostHeartbeats <= hbLossThreshold) {
                    log.debug("Heartbeat loss detected [lost={}, loss-threshold={}, interval={}, node={}]",
                        lostHeartbeats, hbLossThreshold, hbInterval, address);
                }
            }
        }

        public synchronized void onHeartbeatReceived() {
            lostHeartbeats = -1;
            connectFailure = false;
        }

        public synchronized boolean onConnectFailure() {
            if (connectFailure) {
                return false;
            } else {
                connectFailure = true;

                return true;
            }
        }

        public synchronized boolean isAlive() {
            return !connectFailure && lostHeartbeats <= hbLossThreshold;
        }

        public ClusterAddress address() {
            return address;
        }

        @Override
        public synchronized String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(DefaultFailureDetector.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    @ToStringIgnore
    private final int failureQuorum;

    private final int hbLossThreshold;

    private final long hbInterval;

    private final boolean failFast;

    @ToStringIgnore
    private final ReentrantReadWriteLock.ReadLock readLock;

    @ToStringIgnore
    private final ReentrantReadWriteLock.WriteLock writeLock;

    @ToStringIgnore
    private final Map<ClusterAddress, NodeMonitor> monitors = new HashMap<>();

    @ToStringIgnore
    private ClusterAddress localNode;

    @ToStringIgnore
    private Set<ClusterAddress> allNodes = new HashSet<>();

    /**
     * Constructs a new instance with the default configuration options.
     *
     * <p>
     * Please see the documentation of {@link DefaultFailureDetectorConfig} for information about default values.
     * </p>
     */
    public DefaultFailureDetector() {
        this(new DefaultFailureDetectorConfig());
    }

    /**
     * Constructs a new instance using the specified configuration.
     *
     * @param cfg Configuration.
     */
    public DefaultFailureDetector(DefaultFailureDetectorConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(getClass());

        check.positive(cfg.getHeartbeatLossThreshold(), "heartbeat loss threshold");
        check.positive(cfg.getHeartbeatInterval(), "heartbeat interval");
        check.positive(cfg.getFailureDetectionQuorum(), "failure detection quorum");

        hbLossThreshold = cfg.getHeartbeatLossThreshold();
        hbInterval = cfg.getHeartbeatInterval();
        failureQuorum = cfg.getFailureDetectionQuorum();
        failFast = cfg.isFailFast();

        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        readLock = lock.readLock();
        writeLock = lock.writeLock();
    }

    @Override
    public void initialize(FailureDetectorContext context) throws HekateException {
        ArgAssert.notNull(context, "Context");

        writeLock.lock();

        try {
            localNode = context.localAddress();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public long heartbeatInterval() {
        return hbInterval;
    }

    @Override
    public int failureQuorum() {
        return failureQuorum;
    }

    @Override
    public void terminate() {
        writeLock.lock();

        try {
            allNodes.clear();
            monitors.clear();

            localNode = null;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean isAlive(ClusterAddress node) {
        ArgAssert.notNull(node, "Node");

        readLock.lock();

        try {
            NodeMonitor monitor = monitors.get(node);

            return monitor == null || monitor.isAlive();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<ClusterAddress> heartbeatTick() {
        writeLock.lock();

        try {
            doUpdateMonitors(Collections.emptyMap());

            return monitors.values().stream()
                .map(monitor -> {
                    if (DEBUG) {
                        log.debug("Sending heartbeat [target={}]", monitor);
                    }

                    monitor.onHeartbeatSent();

                    return monitor.address();
                })
                .collect(toList());
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean onHeartbeatRequest(ClusterAddress from) {
        if (DEBUG) {
            log.debug("Sending heartbeat reply [to={}]", from);
        }

        return true;
    }

    @Override
    public void onHeartbeatReply(ClusterAddress node) {
        ArgAssert.notNull(node, "Node");

        boolean updateMonitors = false;

        readLock.lock();

        try {
            NodeMonitor monitor = monitors.get(node);

            if (monitor != null) {
                if (DEBUG) {
                    log.debug("Heartbeat reply received [from={}]", monitor);
                }

                updateMonitors = !monitor.isAlive();

                monitor.onHeartbeatReceived();
            } else {
                if (DEBUG) {
                    log.debug("Ignored heartbeat reply from non-monitored node [from={}]", node);
                }
            }
        } finally {
            readLock.unlock();
        }

        if (updateMonitors) {
            writeLock.lock();

            try {
                updateMonitors();
            } finally {
                writeLock.unlock();
            }
        }
    }

    @Override
    public void onConnectFailure(ClusterAddress node) {
        ArgAssert.notNull(node, "Node");

        if (failFast) {
            boolean updateMonitors = false;

            readLock.lock();

            try {
                NodeMonitor monitor = monitors.get(node);

                if (monitor != null) {
                    if (DEBUG) {
                        log.debug("Can't connect to cluster node [node={}]", node);
                    }

                    updateMonitors = monitor.onConnectFailure();
                } else {
                    if (DEBUG) {
                        log.debug("Ignored connection failure to a non-monitored node [node={}]", node);
                    }
                }
            } finally {
                readLock.unlock();
            }

            if (updateMonitors) {
                writeLock.lock();

                try {
                    updateMonitors();
                } finally {
                    writeLock.unlock();
                }
            }
        }
    }

    @Override
    public void update(Set<ClusterAddress> nodes) {
        ArgAssert.notNull(nodes, "Node list");
        ArgAssert.isTrue(nodes.contains(localNode), "Node list doesn't contain the local node.");

        writeLock.lock();

        try {
            if (!nodes.equals(allNodes)) {
                allNodes = new HashSet<>(nodes);

                updateMonitors();
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Returns the amount of heartbeats that can be lost before considering a node failure.
     *
     * @return Amount of heartbeats that can be lost before considering a node failure.
     *
     * @see DefaultFailureDetectorConfig#setHeartbeatLossThreshold(int)
     */
    public int heartbeatLossThreshold() {
        return hbLossThreshold;
    }

    /**
     * Returns addresses of all nodes that are monitored by this failure detector.
     *
     * @return Addresses of all nodes that are monitored by this failure detector.
     */
    public List<ClusterAddress> monitored() {
        readLock.lock();

        try {
            return new ArrayList<>(monitors.keySet());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public DefaultFailureDetectorJmx jmx() {
        return new DefaultFailureDetectorJmx() {
            @Override
            public long getHeartbeatInterval() {
                return heartbeatInterval();
            }

            @Override
            public int getHeartbeatLossThreshold() {
                return heartbeatLossThreshold();
            }

            @Override
            public int getFailureDetectionQuorum() {
                return failureQuorum();
            }

            @Override
            public List<String> getMonitoredAddresses() {
                return monitored().stream().map(ClusterAddress::toString).collect(toList());
            }
        };
    }

    /**
     * Returns {@code true} if the specified node is in the list of {@link #monitored()} nodes.
     *
     * @param node Node to check.
     *
     * @return {@code true} if the specified node is in the list of {@link #monitored()} nodes.
     */
    public boolean isMonitored(ClusterAddress node) {
        ArgAssert.notNull(node, "Node");

        readLock.lock();

        try {
            return monitors.containsKey(node);
        } finally {
            readLock.unlock();
        }
    }

    private void updateMonitors() {
        assert writeLock.isHeldByCurrentThread() : "Thread must hold write lock: " + Thread.currentThread().getName();

        Map<ClusterAddress, NodeMonitor> oldMonitors;

        if (monitors.isEmpty()) {
            oldMonitors = Collections.emptyMap();
        } else {
            oldMonitors = new HashMap<>(monitors);
        }

        monitors.clear();

        doUpdateMonitors(oldMonitors);
    }

    private void doUpdateMonitors(Map<ClusterAddress, NodeMonitor> oldMonitors) {
        assert writeLock.isHeldByCurrentThread() : "Thread must hold write lock: " + Thread.currentThread().getName();

        if (allNodes.size() > 1) {
            long aliveMonitors = monitors.values().stream().filter(NodeMonitor::isAlive).count();

            long missingMonitors = failureQuorum - aliveMonitors;

            if (missingMonitors > 0) {
                addMonitors(missingMonitors, oldMonitors);
            } else if (missingMonitors < 0) {
                removeMonitors(-missingMonitors);
            }
        }

        if (DEBUG && !oldMonitors.isEmpty()) {
            oldMonitors.keySet().forEach(n -> {
                if (!monitors.containsKey(n)) {
                    log.debug("Stopped monitoring [node={}]", n);
                }
            });
        }
    }

    private void addMonitors(long limit, Map<ClusterAddress, NodeMonitor> oldMonitors) {
        assert writeLock.isHeldByCurrentThread() : "Thread must hold write lock: " + Thread.currentThread().getName();

        Consumer<ClusterAddress> doAddNewOrKeepOld = node -> {
            NodeMonitor oldMonitor = oldMonitors.get(node);

            if (oldMonitor == null) {
                if (DEBUG) {
                    log.debug("Started monitoring [node={}]", node);
                }

                monitors.put(node, new NodeMonitor(node, hbLossThreshold, hbInterval));
            } else {
                monitors.put(node, oldMonitor);
            }
        };

        NavigableSet<ClusterAddress> allSorted = new TreeSet<>(allNodes);

        // Nodes that are after the local node in the ring.
        List<ClusterAddress> toAdd = allSorted.tailSet(localNode, false).stream()
            .filter(node -> !monitors.containsKey(node))
            .limit(limit)
            .collect(toList());

        toAdd.forEach(doAddNewOrKeepOld);

        long addMore = limit - toAdd.size();

        if (addMore > 0) {
            // Nodes that are before the local node in the ring.
            allSorted.headSet(localNode, false).stream()
                .filter(node -> !monitors.containsKey(node))
                .limit(addMore)
                .forEach(doAddNewOrKeepOld);
        }
    }

    private void removeMonitors(long limit) {
        assert writeLock.isHeldByCurrentThread() : "Thread must hold write lock: " + Thread.currentThread().getName();

        Consumer<ClusterAddress> doRemove = node -> {
            if (DEBUG) {
                log.debug("Stopped monitoring [node={}]", node);
            }

            monitors.remove(node);
        };

        NavigableSet<ClusterAddress> allSorted = new TreeSet<>(allNodes);

        // Nodes that are before the local node in the ring.
        List<ClusterAddress> toRemove = allSorted.headSet(localNode, false).stream()
            .filter(monitors::containsKey)
            .limit(limit)
            .collect(toList());

        toRemove.forEach(doRemove);

        long removeMore = limit - toRemove.size();

        if (removeMore > 0) {
            allSorted.tailSet(localNode, false).stream()
                .filter(monitors::containsKey)
                .limit(limit)
                .forEach(doRemove);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
