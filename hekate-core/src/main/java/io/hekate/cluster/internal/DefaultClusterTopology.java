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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterNodeIdSupport;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologySupport;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToStringIgnore;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.Spliterator;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyNavigableSet;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableNavigableSet;
import static java.util.Collections.unmodifiableSet;

public final class DefaultClusterTopology implements ClusterTopology, Serializable {
    private static final Comparator<ClusterNode> JOIN_ORDER_COMPARATOR = Comparator.comparingInt(ClusterNode::joinOrder);

    private static final Comparator<ClusterNodeIdSupport> HAS_NODE_ID_COMPARATOR = Comparator.comparing(ClusterNodeIdSupport::id);

    private static final long serialVersionUID = 1;

    private static final ClusterHash EMPTY_HASH = new DefaultClusterHash(emptySet());

    private static final ClusterNode NOT_A_NODE = (ClusterNode)Proxy.newProxyInstance(ClusterNode.class.getClassLoader(),
        new Class[]{ClusterNode.class}, (proxy, method, args) -> {
            throw new AssertionError("This instance should never be exposed to public API.");
        });

    private final long version;

    private final List<ClusterNode> nodes;

    @ToStringIgnore
    private transient ClusterNode localNodeCache;

    @ToStringIgnore
    private transient Set<ClusterNode> nodeSetCache;

    @ToStringIgnore
    private transient ClusterNode oldestNodeCache;

    @ToStringIgnore
    private transient ClusterNode youngestNodeCache;

    @ToStringIgnore
    private transient List<ClusterNode> remoteNodesCache;

    @ToStringIgnore
    private transient NavigableSet<ClusterNode> joinOrderCache;

    @ToStringIgnore
    private transient ClusterHash hashCache;

    private DefaultClusterTopology(long version, Set<ClusterNode> nodes) {
        this(version, new ArrayList<>(nodes), false);
    }

    private DefaultClusterTopology(long version, List<ClusterNode> nodes, boolean safe) {
        assert nodes != null : "Nodes list is null.";

        this.version = version;

        if (safe) {
            this.nodes = nodes;
        } else {
            switch (nodes.size()) {
                case 0: {
                    this.nodes = emptyList();

                    break;
                }
                case 1: {
                    this.nodes = singletonList(nodes.get(0));

                    break;
                }
                default: {
                    List<ClusterNode> copy = new ArrayList<>(nodes);

                    this.nodes = sortedUnmodifiableList(copy);
                }
            }
        }
    }

    public static DefaultClusterTopology of(long version, Set<ClusterNode> nodes) {
        return new DefaultClusterTopology(version, new ArrayList<>(nodes), false);
    }

    public static DefaultClusterTopology empty() {
        return new DefaultClusterTopology(0, emptyList(), true);
    }

    public DefaultClusterTopology updateIfModified(Set<ClusterNode> newNodes) {
        if (!nodeSet().equals(newNodes)) {
            return update(newNodes);
        }

        return this;
    }

    public DefaultClusterTopology update(Set<ClusterNode> newNodes) {
        return new DefaultClusterTopology(version + 1, newNodes);
    }

    @Override
    public long version() {
        return version;
    }

    @Override
    public ClusterHash hash() {
        ClusterHash hash = this.hashCache;

        if (hash == null) {
            if (nodes.isEmpty()) {
                hash = EMPTY_HASH;
            } else {
                hash = new DefaultClusterHash(nodes);
            }

            this.hashCache = hash;
        }

        return hash;
    }

    @Override
    public ClusterNode localNode() {
        ClusterNode localNode = this.localNodeCache;

        if (localNode == null) {
            if (nodes.isEmpty()) {
                localNode = NOT_A_NODE;
            } else {
                for (ClusterNode node : nodes) {
                    if (node.isLocal()) {
                        localNode = node;

                        break;
                    }
                }

                if (localNode == null) {
                    localNode = NOT_A_NODE;
                }
            }

            localNodeCache = localNode;
        }

        return localNode == NOT_A_NODE ? null : localNode;
    }

    @Override
    public List<ClusterNode> nodes() {
        return nodes;
    }

    @Override
    public ClusterNode first() {
        return nodes.isEmpty() ? null : nodes.get(0);
    }

    @Override
    public ClusterNode last() {
        return nodes.isEmpty() ? null : nodes.get(nodes.size() - 1);
    }

    @Override
    public Set<ClusterNode> nodeSet() {
        Set<ClusterNode> nodeSet = this.nodeSetCache;

        if (nodeSet == null) {
            if (nodes.isEmpty()) {
                nodeSet = emptySet();
            } else if (nodes.size() == 1) {
                nodeSet = singleton(nodes.get(0));
            } else {
                nodeSet = unmodifiableSet(new HashSet<>(nodes));
            }

            this.nodeSetCache = nodeSet;
        }

        return nodeSet;
    }

    @Override
    public NavigableSet<ClusterNode> joinOrder() {
        NavigableSet<ClusterNode> joinOrder = this.joinOrderCache;

        if (joinOrder == null) {
            if (nodes.isEmpty()) {
                joinOrder = emptyNavigableSet();
            } else {
                joinOrder = new TreeSet<>(JOIN_ORDER_COMPARATOR);

                joinOrder.addAll(nodes);

                joinOrder = unmodifiableNavigableSet(joinOrder);
            }

            this.joinOrderCache = joinOrder;
        }

        return joinOrder;
    }

    @Override
    public Stream<ClusterNode> stream() {
        return nodes.stream();
    }

    @Override
    public Iterator<ClusterNode> iterator() {
        return nodes.iterator();
    }

    @Override
    public void forEach(Consumer<? super ClusterNode> action) {
        nodes.forEach(action);
    }

    @Override
    public Spliterator<ClusterNode> spliterator() {
        return nodes.spliterator();
    }

    @Override
    public List<ClusterNode> remoteNodes() {
        List<ClusterNode> remoteNodes = this.remoteNodesCache;

        if (remoteNodes == null) {
            int size = nodes.size();

            switch (size) {
                case 0: {
                    remoteNodes = emptyList();

                    break;
                }
                case 1: {
                    ClusterNode node = nodes.get(0);

                    if (node.isLocal()) {
                        remoteNodes = emptyList();
                    } else {
                        remoteNodes = singletonList(node);
                    }

                    break;
                }
                default: {
                    for (ClusterNode node : nodes) {
                        if (!node.isLocal()) {
                            if (remoteNodes == null) {
                                remoteNodes = new ArrayList<>(size);
                            }

                            remoteNodes.add(node);
                        }
                    }

                    if (remoteNodes == null) {
                        remoteNodes = emptyList();
                    } else {
                        remoteNodes = unmodifiableList(remoteNodes);
                    }
                }
            }

            this.remoteNodesCache = remoteNodes;
        }

        return remoteNodes;
    }

    @Override
    public boolean contains(ClusterNode node) {
        return Collections.binarySearch(nodes, node) >= 0;
    }

    @Override
    public boolean contains(ClusterNodeId id) {
        return Collections.binarySearch(nodes, id, HAS_NODE_ID_COMPARATOR) >= 0;
    }

    @Override
    public ClusterNode get(ClusterNodeId id) {
        int idx = Collections.binarySearch(nodes, id, HAS_NODE_ID_COMPARATOR);

        if (idx < 0) {
            return null;
        }

        return nodes.get(idx);
    }

    @Override
    public int size() {
        return nodes.size();
    }

    @Override
    public boolean isEmpty() {
        return nodes.isEmpty();
    }

    @Override
    public ClusterNode oldest() {
        ClusterNode oldest = this.oldestNodeCache;

        if (oldest == null) {
            switch (nodes.size()) {
                case 0: {
                    oldest = NOT_A_NODE;

                    break;
                }
                case 1: {
                    oldest = nodes.get(0);

                    break;
                }
                default: {
                    for (ClusterNode node : nodes) {
                        if (oldest == null || oldest.joinOrder() > node.joinOrder()) {
                            oldest = node;
                        }
                    }
                }
            }

            this.oldestNodeCache = oldest;
        }

        return oldest == NOT_A_NODE ? null : oldest;
    }

    @Override
    public ClusterNode youngest() {
        ClusterNode youngest = this.youngestNodeCache;

        if (youngest == null) {
            switch (nodes.size()) {
                case 0: {
                    youngest = NOT_A_NODE;

                    break;
                }
                case 1: {
                    youngest = nodes.get(0);

                    break;
                }
                default: {
                    for (ClusterNode node : nodes) {
                        if (youngest == null || youngest.joinOrder() < node.joinOrder()) {
                            youngest = node;
                        }
                    }
                }
            }

            this.youngestNodeCache = youngest;
        }

        return youngest == NOT_A_NODE ? null : youngest;
    }

    @Override
    public ClusterNode random() {
        if (nodes.isEmpty()) {
            return null;
        } else {
            int size = nodes.size();

            if (size == 1) {
                return nodes.get(0);
            } else {
                return nodes.get(ThreadLocalRandom.current().nextInt(size));
            }
        }
    }

    @Override
    public ClusterTopology filterAll(ClusterFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        if (nodes.isEmpty()) {
            return this;
        }

        List<ClusterNode> filtered = filter.apply(nodes);

        if (filtered.size() == nodes.size()) {
            return this;
        }

        return new DefaultClusterTopology(version, filtered, false);
    }

    @Override
    public ClusterTopology filter(ClusterNodeFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        List<ClusterNode> filtered = null;

        int size = nodes.size();

        for (ClusterNode node : nodes) {
            if (filter.accept(node)) {
                if (filtered == null) {
                    filtered = new ArrayList<>(size);
                }

                filtered.add(node);
            }
        }

        if (filtered == null) {
            filtered = emptyList();
        } else if (filtered.size() == size) {
            // Minor optimization to preserve values cached within this instance.
            return this;
        } else {
            filtered = sortedUnmodifiableList(filtered);
        }

        return new DefaultClusterTopology(version, filtered, true);
    }

    /**
     * Returns this (inherited from {@link ClusterTopologySupport}).
     *
     * @return This instance.
     */
    @Override
    public ClusterTopology topology() {
        return this;
    }

    private List<ClusterNode> sortedUnmodifiableList(List<ClusterNode> copy) {
        copy.sort(ClusterNode::compareTo);

        return unmodifiableList(copy);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ClusterTopology)) {
            return false;
        }

        ClusterTopology that = (ClusterTopology)o;

        return nodes.equals(that.nodes());

    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder(ClusterTopology.class.getSimpleName())
            .append("[size=").append(nodes.size())
            .append(", version=").append(version);

        if (!nodes.isEmpty()) {
            buf.append(", nodes=").append(nodes);
        }

        buf.append(']');

        return buf.toString();
    }
}
