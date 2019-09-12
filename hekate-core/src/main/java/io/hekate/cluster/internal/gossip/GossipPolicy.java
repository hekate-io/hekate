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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterNodeId;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import static java.util.stream.Collectors.toList;

public interface GossipPolicy {
    GossipPolicy RANDOM_PREFER_UNSEEN = new GossipPolicy() {
        @Override
        public Collection<GossipNodeState> selectNodes(
            int size,
            GossipNodeState fromNode,
            List<GossipNodeState> nodes,
            Set<ClusterNodeId> seen
        ) {
            assert size > 0 : "Size must be above zero [size=" + size + ']';
            assert fromNode != null : "From node is null.";
            assert seen != null : "Seen list is null.";

            // Select unseen nodes with higher probability.
            boolean preferUnseen = ThreadLocalRandom.current().nextInt(100) <= 70;

            if (preferUnseen) {
                List<GossipNodeState> unseen = nodes.stream()
                    .filter(n -> !seen.contains(n.id()))
                    .collect(toList());

                if (!unseen.isEmpty()) {
                    nodes = unseen;
                }
            }

            if (nodes.size() <= size) {
                return nodes;
            }

            List<GossipNodeState> liveNodes = nodes.stream()
                .filter(n -> !n.status().isTerminated())
                .collect(toList());

            if (liveNodes.size() == size) {
                return liveNodes;
            }

            if (liveNodes.size() > size) {
                nodes = liveNodes;
            }

            Collection<GossipNodeState> selected = new HashSet<>(size, 1.0f);

            int nodesSize = nodes.size();

            while (selected.size() < size) {
                selected.add(nodes.get(ThreadLocalRandom.current().nextInt(nodesSize)));
            }

            return selected;
        }

        @Override
        public String toString() {
            return "RANDOM_PREFER_UNSEEN";
        }
    };

    GossipPolicy RANDOM_UNSEEN_NON_DOWN = new GossipPolicy() {
        @Override
        public Collection<GossipNodeState> selectNodes(
            int size,
            GossipNodeState fromNode,
            List<GossipNodeState> nodes,
            Set<ClusterNodeId> seen
        ) {
            assert size > 0 : "Size must be above zero [size=" + size + ']';
            assert fromNode != null : "From node is null.";
            assert seen != null : "Seen list is null.";

            nodes = nodes.stream()
                .filter(n -> !seen.contains(n.id()) && !n.status().isTerminated())
                .collect(toList());

            if (nodes.size() <= size) {
                return nodes;
            }

            Collection<GossipNodeState> selected = new HashSet<>(size, 1.0f);

            int nodesSize = nodes.size();

            while (selected.size() < size) {
                selected.add(nodes.get(ThreadLocalRandom.current().nextInt(nodesSize)));
            }

            return selected;
        }

        @Override
        public String toString() {
            return "RANDOM_UNSEEN_NON_DOWN";
        }
    };

    GossipPolicy RANDOM_UNSEEN = new GossipPolicy() {
        @Override
        public Collection<GossipNodeState> selectNodes(
            int size,
            GossipNodeState fromNode,
            List<GossipNodeState> nodes,
            Set<ClusterNodeId> seen
        ) {
            assert size > 0 : "Size must be above zero [size=" + size + ']';
            assert fromNode != null : "From node is null.";
            assert seen != null : "Seen list is null.";

            nodes = nodes.stream()
                .filter(n -> !seen.contains(n.id()))
                .collect(toList());

            if (nodes.size() <= size) {
                return nodes;
            }

            Collection<GossipNodeState> selected = new HashSet<>(size, 1.0f);

            int nodesSize = nodes.size();

            while (selected.size() < size) {
                selected.add(nodes.get(ThreadLocalRandom.current().nextInt(nodesSize)));
            }

            return selected;
        }

        @Override
        public String toString() {
            return "RANDOM_UNSEEN";
        }
    };

    GossipPolicy ON_DOWN = new GossipPolicy() {
        @Override
        public Collection<GossipNodeState> selectNodes(
            int size,
            GossipNodeState fromNode,
            List<GossipNodeState> nodes,
            Set<ClusterNodeId> seen
        ) {
            assert size > 0 : "Size must be above zero [size=" + size + ']';
            assert fromNode != null : "From node is null.";
            assert seen != null : "Seen list is null.";

            // Select target nodes by using a round robin approach.
            NavigableSet<GossipNodeState> sorted = new TreeSet<>(nodes);

            Set<GossipNodeState> targets = new HashSet<>(size, 1.0f);

            sorted.tailSet(fromNode, false).stream()
                .limit(size)
                .forEach(targets::add);

            if (targets.size() < size) {
                sorted.headSet(fromNode, false).stream()
                    .limit(size - targets.size())
                    .forEach(targets::add);
            }

            // If all selected nodes are DOWN, FAILED or LEAVING then try to add any other alive node.
            Predicate<GossipNodeState> isLiveNode = s -> !s.status().isTerminated() && s.status() != GossipNodeStatus.LEAVING;

            if (!targets.isEmpty() && targets.stream().noneMatch(isLiveNode)) {
                List<GossipNodeState> nonDown = nodes.stream().filter(isLiveNode).collect(toList());

                if (!nonDown.isEmpty()) {
                    targets.add(nonDown.get(ThreadLocalRandom.current().nextInt(nonDown.size())));
                }
            }

            return targets;
        }

        @Override
        public String toString() {
            return "ON_DOWN";
        }
    };

    Collection<GossipNodeState> selectNodes(int size, GossipNodeState fromNode, List<GossipNodeState> allNodes, Set<ClusterNodeId> seen);
}
