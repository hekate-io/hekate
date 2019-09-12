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

package io.hekate.cluster;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.service.Service;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Predefined cluster filters.
 */
public final class ClusterFilters {
    private static class RingFilter implements ClusterFilter {
        private final Comparator<ClusterNode> comparator;

        public RingFilter(Comparator<ClusterNode> comparator) {
            this.comparator = comparator;
        }

        @Override
        public List<ClusterNode> apply(List<ClusterNode> nodes) {
            int size = nodes.size();

            switch (size) {
                case 0: {
                    return emptyList();
                }
                case 1: {
                    ClusterNode node = nodes.get(0);

                    return node.isLocal() ? nodes : emptyList();
                }
                default: {
                    ClusterNode local = null;

                    NavigableSet<ClusterNode> sortedNodes = new TreeSet<>(comparator);

                    for (ClusterNode node : nodes) {
                        if (node.isLocal()) {
                            local = node;
                        }

                        sortedNodes.add(node);
                    }

                    if (local == null) {
                        return emptyList();
                    } else {
                        ClusterNode next = sortedNodes.higher(local);

                        if (next == null) {
                            next = sortedNodes.first();
                        }

                        return singletonList(next);
                    }
                }
            }
        }
    }

    private static final Comparator<ClusterNode> JOIN_ORDER = Comparator.comparingInt(ClusterNode::joinOrder);

    private static final Comparator<ClusterNode> NATURAL_ORDER = ClusterNode::compareTo;

    private static final ClusterFilter REMOTES = forFilter(ClusterNode::isRemote);

    private static final ClusterFilter NEXT = new RingFilter(NATURAL_ORDER);

    private static final ClusterFilter NEXT_IN_JOIN_ORDER = new RingFilter(JOIN_ORDER);

    private static final ClusterFilter OLDEST = nodes -> {
        if (nodes.isEmpty()) {
            return emptyList();
        }

        ClusterNode oldest = null;

        for (ClusterNode node : nodes) {
            if (oldest == null || oldest.joinOrder() > node.joinOrder()) {
                oldest = node;
            }
        }

        return singletonList(oldest);
    };

    private static final ClusterFilter YOUNGEST = nodes -> {
        if (nodes.isEmpty()) {
            return emptyList();
        }

        ClusterNode youngest = null;

        for (ClusterNode node : nodes) {
            if (youngest == null || youngest.joinOrder() < node.joinOrder()) {
                youngest = node;
            }
        }

        return singletonList(youngest);
    };

    private ClusterFilters() {
        // No-op.
    }

    /**
     * Filters nodes using a ring structure based on natural nodes ordering.
     *
     * <p>
     * This filter uses the {@link ClusterNode#compareTo(ClusterNode) natural sort order} to organize all cluster nodes as a ring and
     * selects a node which is next to the {@link ClusterNode#isLocal() local node}. If local node is not within the cluster topology
     * then empty set is returned.
     * </p>
     *
     * @return Filter.
     */
    public static ClusterFilter forNext() {
        return NEXT;
    }

    /**
     * Filters nodes using a ring structure based on nodes join order.
     *
     * <p>
     * This filter organizes all cluster nodes as a ring in their {@link ClusterNode#joinOrder()} and selects a node which is next to the
     * {@link ClusterNode#isLocal() local node}. If local node is not within the cluster topology then empty set is returned.
     * </p>
     *
     * @return Filter.
     */
    public static ClusterFilter forNextInJoinOrder() {
        return NEXT_IN_JOIN_ORDER;
    }

    /**
     * Filters out all nodes but the specified one.
     *
     * @param nodeId Node identifier.
     *
     * @return Filter.
     */
    public static ClusterFilter forNode(ClusterNodeId nodeId) {
        ArgAssert.notNull(nodeId, "Node");

        return nodes -> {
            if (!nodes.isEmpty()) {
                for (ClusterNode node : nodes) {
                    if (node.id().equals(nodeId)) {
                        return singletonList(node);
                    }
                }
            }

            return emptyList();
        };
    }

    /**
     * Filters out all nodes but the specified one.
     *
     * @param node Node.
     *
     * @return Filter.
     */
    public static ClusterFilter forNode(ClusterNode node) {
        ArgAssert.notNull(node, "Node");

        return nodes -> {
            if (nodes.isEmpty() || !nodes.contains(node)) {
                return emptyList();
            } else {
                return singletonList(node);
            }
        };
    }

    /**
     * Filters out all nodes but the oldest one (with the smallest {@link ClusterNode#joinOrder()}).
     *
     * @return Filter.
     */
    public static ClusterFilter forOldest() {
        return OLDEST;
    }

    /**
     * Filters out all nodes but the youngest one (with the largest {@link ClusterNode#joinOrder()}).
     *
     * @return Filter.
     */
    public static ClusterFilter forYoungest() {
        return YOUNGEST;
    }

    /**
     * Wraps the specified filter with {@link ClusterFilter}.
     *
     * @param filter Filter.
     *
     * @return Group filter.
     */
    public static ClusterFilter forFilter(ClusterNodeFilter filter) {
        ArgAssert.notNull(filter, "Filter");

        return nodes -> {
            List<ClusterNode> result = null;

            for (ClusterNode node : nodes) {
                if (filter.accept(node)) {
                    if (result == null) {
                        int size = nodes.size();

                        if (size == 1) {
                            return singletonList(node);
                        }

                        result = new ArrayList<>(size);
                    }

                    result.add(node);
                }
            }

            return result != null ? result : emptyList();
        };
    }

    /**
     * Filters out all but nodes with the specified {@link ClusterNode#roles() role}.
     *
     * @param role Role (see {@link ClusterNode#hasRole(String)}).
     *
     * @return Filter.
     */
    public static ClusterFilter forRole(String role) {
        ArgAssert.notNull(role, "Role");

        return forFilter(n -> n.hasRole(role));
    }

    /**
     * Filters out all but nodes with the specified {@link ClusterNode#properties() property}.
     *
     * @param name Property name (see {@link ClusterNode#hasProperty(String)}).
     *
     * @return Filter.
     */
    public static ClusterFilter forProperty(String name) {
        ArgAssert.notNull(name, "Property name");

        return forFilter(n -> n.hasProperty(name));
    }

    /**
     * Filters out all but nodes with the specified {@link ClusterNode#property(String) property} value.
     *
     * @param name Property name.
     * @param value Property value.
     *
     * @return Filter.
     */
    public static ClusterFilter forProperty(String name, String value) {
        ArgAssert.notNull(name, "Property name");
        ArgAssert.notNull(value, "Property value");

        return forFilter(n -> value.equals(n.property(name)));
    }

    /**
     * Filters out all but nodes with the specified {@link ClusterNode#services()} service}.
     *
     * @param type Service type (see {@link ClusterNode#hasService(Class)}).
     *
     * @return Filter.
     */
    public static ClusterFilter forService(Class<? extends Service> type) {
        ArgAssert.notNull(type, "Service type");

        return forFilter(n -> n.hasService(type));
    }

    /**
     * Filters out all but {@link ClusterNode#isLocal() remote} nodes.
     *
     * @return Filter.
     */
    public static ClusterFilter forRemotes() {
        return REMOTES;
    }
}
