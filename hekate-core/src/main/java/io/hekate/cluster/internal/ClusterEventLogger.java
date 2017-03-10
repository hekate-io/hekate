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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterEventLogger implements ClusterEventListener {
    private static final Comparator<ClusterNode> NODES_BY_ADDRESS = (o1, o2) -> {
        InetSocketAddress a1 = o1.getNetAddress();
        InetSocketAddress a2 = o2.getNetAddress();

        int cmp = a1.getAddress().getHostAddress().compareTo(a2.getAddress().getHostAddress());

        if (cmp == 0) {
            cmp = Integer.compare(a1.getPort(), a2.getPort());
        }

        if (cmp == 0) {
            cmp = o1.getId().compareTo(o2.getId());
        }

        return cmp;
    };

    private static final Logger log = LoggerFactory.getLogger(ClusterEventLogger.class);

    @Override
    public void onEvent(ClusterEvent event) {
        if (log.isInfoEnabled()) {
            switch (event.getType()) {
                case JOIN: {
                    log.info("{}", toDetailedString("Join topology", event.getTopology(), null, null));

                    break;
                }
                case LEAVE: {
                    ClusterLeaveEvent leave = event.asLeave();

                    Set<ClusterNode> added = leave.getAdded();
                    Set<ClusterNode> removed = leave.getRemoved();

                    log.info("{}", toDetailedString("Leave topology", event.getTopology(), added, removed));

                    break;
                }
                case CHANGE: {
                    ClusterChangeEvent change = event.asChange();

                    Set<ClusterNode> added = change.getAdded();
                    Set<ClusterNode> removed = change.getRemoved();

                    log.info("{}", toDetailedString("Topology change", event.getTopology(), added, removed));

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected event type: " + event.getType());
                }
            }
        }
    }

    private String toDetailedString(String eventType, ClusterTopology topology, Set<ClusterNode> added, Set<ClusterNode> removed) {
        String nl = System.lineSeparator();

        StringBuilder buf = new StringBuilder();

        buf.append(nl);

        List<ClusterNode> allNodes = new ArrayList<>(topology.getNodes());

        if (removed != null) {
            allNodes.addAll(removed);
        }

        buf.append("*** ").append(eventType);
        buf.append(" [size=").append(topology.size()).append(", version=").append(topology.getVersion()).append("] {").append(nl);

        allNodes.stream().sorted(NODES_BY_ADDRESS).forEach(n -> {
            buf.append("      ");

            if (n.isLocal()) {
                buf.append("   this -> ");
            } else if (added != null && added.contains(n)) {
                buf.append("  added -> ");
            } else if (removed != null && removed.contains(n)) {
                buf.append("removed -> ");
            } else {
                buf.append("           ");
            }

            buf.append('[').append(ToString.formatProperties(n)).append(']').append(nl);
        });

        buf.append("    }");

        return buf.toString();
    }
}
