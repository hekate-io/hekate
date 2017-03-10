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

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeFilter;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterTopologyHash;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableNavigableSet;
import static java.util.Collections.unmodifiableSet;

public class DefaultClusterTopology implements ClusterTopology, Serializable {
    public static final Comparator<ClusterNode> JOIN_ORDER_COMPARATOR = Comparator.comparingInt(ClusterNode::getJoinOrder);

    private static final long serialVersionUID = 1;

    private static final ClusterTopologyHash EMPTY_HASH = new DefaultClusterTopologyHash(emptySet());

    private static final ClusterNode NOT_A_NODE = (ClusterNode)Proxy.newProxyInstance(ClusterNode.class.getClassLoader(),
        new Class[]{ClusterNode.class}, (proxy, method, args) -> {
            throw new AssertionError("This instance should never be exposed to public API.");
        });

    private final long version;

    private final Set<ClusterNode> nodes;

    @ToStringIgnore
    private transient ClusterNode localNodeCache;

    @ToStringIgnore
    private transient List<ClusterNode> nodesListCache;

    @ToStringIgnore
    private transient Map<ClusterNodeId, ClusterNode> nodesByIdCache;

    @ToStringIgnore
    private transient ClusterNode oldestNodeCache;

    @ToStringIgnore
    private transient ClusterNode youngestNodeCache;

    @ToStringIgnore
    private transient Set<ClusterNode> remoteNodesCache;

    @ToStringIgnore
    private transient NavigableSet<ClusterNode> joinOrderCache;

    @ToStringIgnore
    private transient List<ClusterNode> joinOrderListCache;

    @ToStringIgnore
    private transient NavigableSet<ClusterNode> sortedCache;

    @ToStringIgnore
    private transient List<ClusterNode> sortedListCache;

    @ToStringIgnore
    private transient ClusterTopologyHash hashCache;

    public DefaultClusterTopology(long version, Set<ClusterNode> nodes) {
        this(version, nodes, false);
    }

    private DefaultClusterTopology(long version, Set<ClusterNode> nodes, boolean safe) {
        assert nodes != null : "Nodes list is null.";

        this.version = version;

        if (safe) {
            this.nodes = nodes;
        } else {
            int size = nodes.size();

            if (size == 0) {
                this.nodes = emptySet();
            } else if (size == 1) {
                this.nodes = singleton(nodes.iterator().next());
            } else {
                Set<ClusterNode> copy = new HashSet<>(size, 1.0f);

                copy.addAll(nodes);

                this.nodes = unmodifiableSet(copy);
            }
        }
    }

    public DefaultClusterTopology updateIfModified(Set<ClusterNode> newNodes) {
        if (!nodes.equals(newNodes)) {
            return update(newNodes);
        }

        return this;
    }

    public DefaultClusterTopology update(Set<ClusterNode> newNodes) {
        return new DefaultClusterTopology(version + 1, newNodes);
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public ClusterTopologyHash getHash() {
        ClusterTopologyHash hash = this.hashCache;

        if (hash == null) {
            if (nodes.isEmpty()) {
                hash = EMPTY_HASH;
            } else {
                hash = new DefaultClusterTopologyHash(nodes);
            }

            this.hashCache = hash;
        }

        return hash;
    }

    @Override
    public ClusterNode getLocalNode() {
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
            }

            localNodeCache = localNode;
        }

        return localNode == NOT_A_NODE ? null : localNode;
    }

    @Override
    public Set<ClusterNode> getNodes() {
        return nodes;
    }

    @Override
    public List<ClusterNode> getNodesList() {
        List<ClusterNode> nodesList = this.nodesListCache;

        if (nodesList == null) {
            if (nodes.isEmpty()) {
                nodesList = emptyList();
            } else {
                nodesList = new ArrayList<>(nodes);
            }

            this.nodesListCache = unmodifiableList(nodesList);
        }

        return nodesList;
    }

    @Override
    public NavigableSet<ClusterNode> getJoinOrder() {
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
    public List<ClusterNode> getJoinOrderList() {
        List<ClusterNode> joinOrderList = this.joinOrderListCache;

        if (joinOrderList == null) {
            this.joinOrderListCache = joinOrderList = getNodesAsSortedList(JOIN_ORDER_COMPARATOR);
        }

        return joinOrderList;
    }

    @Override
    public NavigableSet<ClusterNode> getSorted() {
        NavigableSet<ClusterNode> sorted = this.sortedCache;

        if (sorted == null) {
            if (nodes.isEmpty()) {
                sorted = emptyNavigableSet();
            } else {
                sorted = unmodifiableNavigableSet(new TreeSet<>(nodes));
            }

            this.sortedCache = sorted;
        }

        return sorted;
    }

    @Override
    public List<ClusterNode> getSortedList() {
        List<ClusterNode> sortedList = this.sortedListCache;

        if (sortedList == null) {
            this.sortedListCache = sortedList = getNodesAsSortedList(null);
        }

        return sortedList;
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
    public Set<ClusterNode> getRemoteNodes() {
        Set<ClusterNode> remoteNodes = this.remoteNodesCache;

        if (remoteNodes == null) {
            int size = nodes.size();

            if (size == 0) {
                remoteNodes = emptySet();
            } else if (size == 1) {
                ClusterNode node = nodes.iterator().next();

                if (node.isLocal()) {
                    remoteNodes = emptySet();
                } else {
                    remoteNodes = singleton(node);
                }
            } else {
                for (ClusterNode node : nodes) {
                    if (!node.isLocal()) {
                        if (remoteNodes == null) {
                            remoteNodes = new HashSet<>(size, 1.0f);
                        }

                        remoteNodes.add(node);
                    }
                }

                if (remoteNodes == null) {
                    remoteNodes = emptySet();
                } else {
                    remoteNodes = unmodifiableSet(remoteNodes);
                }
            }

            this.remoteNodesCache = remoteNodes;
        }

        return remoteNodes;
    }

    @Override
    public boolean contains(ClusterNode node) {
        return nodes.contains(node);
    }

    @Override
    public boolean contains(ClusterNodeId node) {
        return getNodesById().containsKey(node);
    }

    @Override
    public ClusterNode get(ClusterNodeId id) {
        return getNodesById().get(id);
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
    public ClusterNode getOldest() {
        ClusterNode oldest = this.oldestNodeCache;

        if (oldest == null) {
            int size = nodes.size();

            if (size == 0) {
                oldest = NOT_A_NODE;
            } else if (size == 1) {
                oldest = nodes.iterator().next();
            } else if (size > 1) {
                for (ClusterNode node : nodes) {
                    if (oldest == null || oldest.getJoinOrder() > node.getJoinOrder()) {
                        oldest = node;
                    }
                }
            }

            this.oldestNodeCache = oldest;
        }

        return oldest == NOT_A_NODE ? null : oldest;
    }

    @Override
    public ClusterNode getYoungest() {
        ClusterNode youngest = this.youngestNodeCache;

        if (youngest == null) {
            int size = nodes.size();

            if (size == 0) {
                youngest = NOT_A_NODE;
            } else if (size == 1) {
                youngest = nodes.iterator().next();
            } else if (size > 1) {
                for (ClusterNode node : nodes) {
                    if (youngest == null || youngest.getJoinOrder() < node.getJoinOrder()) {
                        youngest = node;
                    }
                }
            }

            this.youngestNodeCache = youngest;
        }

        return youngest == NOT_A_NODE ? null : youngest;
    }

    @Override
    public ClusterNode getRandom() {
        if (nodes.isEmpty()) {
            return null;
        } else {
            List<ClusterNode> nodeList = getNodesList();

            int size = nodeList.size();

            if (size == 1) {
                return nodeList.get(0);
            } else {
                return nodeList.get(ThreadLocalRandom.current().nextInt(size));
            }
        }
    }

    @Override
    public ClusterTopology filterAll(ClusterFilter filter) {
        ArgAssert.check(filter != null, "Filter must be not null.");

        if (nodes.isEmpty()) {
            return this;
        }

        Set<ClusterNode> filtered = filter.apply(nodes);

        if (filtered.size() == nodes.size()) {
            return this;
        }

        return new DefaultClusterTopology(version, filtered);
    }

    @Override
    public ClusterTopology filter(ClusterNodeFilter filter) {
        ArgAssert.check(filter != null, "Filter must be not null.");

        Set<ClusterNode> filtered = null;

        int size = nodes.size();

        for (ClusterNode node : nodes) {
            if (filter.accept(node)) {
                if (filtered == null) {
                    filtered = new HashSet<>(size, 1.0f);
                }

                filtered.add(node);
            }
        }

        if (filtered == null) {
            filtered = emptySet();
        } else if (filtered.size() == size) {
            // Minor optimization to preserve values cached within this instance.
            return this;
        } else {
            filtered = unmodifiableSet(filtered);
        }

        return new DefaultClusterTopology(version, filtered, true);
    }

    private List<ClusterNode> getNodesAsSortedList(Comparator<ClusterNode> comparator) {
        List<ClusterNode> joinOrderList;

        if (nodes.isEmpty()) {
            joinOrderList = emptyList();
        } else if (nodes.size() == 1) {
            joinOrderList = singletonList(nodes.iterator().next());
        } else {
            joinOrderList = new ArrayList<>(nodes);

            joinOrderList.sort(comparator);

            joinOrderList = unmodifiableList(joinOrderList);
        }

        return joinOrderList;
    }

    private Map<ClusterNodeId, ClusterNode> getNodesById() {
        Map<ClusterNodeId, ClusterNode> nodesById = this.nodesByIdCache;

        if (nodesById == null) {
            int size = nodes.size();

            if (size == 0) {
                nodesById = Collections.emptyMap();
            } else if (size == 1) {
                ClusterNode node = nodes.iterator().next();

                nodesById = Collections.singletonMap(node.getId(), node);
            } else {
                nodesById = new HashMap<>(size, 1.0f);

                for (ClusterNode node : nodes) {
                    nodesById.put(node.getId(), node);
                }

                nodesById = unmodifiableMap(nodesById);
            }

            this.nodesByIdCache = nodesById;
        }

        return nodesById;
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

        return nodes.equals(that.getNodes());

    }

    @Override
    public int hashCode() {
        return nodes.hashCode();
    }

    @Override
    public String toString() {
        return ToString.format(ClusterTopology.class, this);
    }
}
