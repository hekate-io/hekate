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

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ClusterEventLogger implements ClusterEventListener {
    private static final Comparator<ClusterNode> NODES_BY_ADDRESS = (o1, o2) -> {
        InetSocketAddress a1 = o1.socket();
        InetSocketAddress a2 = o2.socket();

        int cmp = AddressUtils.host(a1).compareTo(AddressUtils.host(a2));

        if (cmp == 0) {
            cmp = Integer.compare(a1.getPort(), a2.getPort());
        }

        if (cmp == 0) {
            cmp = o1.id().compareTo(o2.id());
        }

        return cmp;
    };

    private static final Logger log = LoggerFactory.getLogger(ClusterEventLogger.class);

    @Override
    public void onEvent(ClusterEvent event) {
        if (log.isInfoEnabled()) {
            switch (event.type()) {
                case JOIN: {
                    log.info("{}", toDetailedString("Join topology", event.topology(), null, null, null));

                    break;
                }
                case LEAVE: {
                    ClusterLeaveEvent leave = event.asLeave();

                    List<ClusterNode> added = leave.added();
                    List<ClusterNode> removed = leave.removed();

                    log.info("{}", toDetailedString("Leave topology", event.topology(), added, removed, null));

                    break;
                }
                case CHANGE: {
                    ClusterChangeEvent change = event.asChange();

                    List<ClusterNode> added = change.added();
                    List<ClusterNode> removed = change.removed();
                    List<ClusterNode> failed = change.failed();

                    log.info("{}", toDetailedString("Topology change", event.topology(), added, removed, failed));

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected event type: " + event.type());
                }
            }
        }
    }

    private String toDetailedString(
        String eventType,
        ClusterTopology topology,
        List<ClusterNode> added,
        List<ClusterNode> removed,
        List<ClusterNode> failed
    ) {
        String nl = System.lineSeparator();

        List<ClusterNode> allNodes = new ArrayList<>(topology.nodes());

        if (removed != null) {
            allNodes.addAll(removed);
        }

        StringBuilder buf = new StringBuilder(256)
            .append(nl)
            .append("*** ").append(eventType)
            .append(" [size=").append(topology.size())
            .append(", version=").append(topology.version())
            .append("] {").append(nl);

        allNodes.stream().sorted(NODES_BY_ADDRESS).forEach(n -> {
            buf.append("      ");

            if (n.isLocal()) {
                buf.append("   this -> ");
            } else if (added != null && added.contains(n)) {
                buf.append("  added -> ");
            } else if (failed != null && failed.contains(n)) {
                buf.append(" failed -> ");
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
