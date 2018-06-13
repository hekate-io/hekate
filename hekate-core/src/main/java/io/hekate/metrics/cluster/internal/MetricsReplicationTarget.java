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

import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessagingChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetricsReplicationTarget {
    private static final Logger log = LoggerFactory.getLogger(MetricsReplicationTarget.class);

    private final ClusterNodeId to;

    private final MessagingChannel<MetricsProtocol> channel;

    private final ClusterNodeId localNode;

    private final Map<ClusterNodeId, Long> sentVersions = new ConcurrentHashMap<>();

    public MetricsReplicationTarget(ClusterNodeId to, MessagingChannel<MetricsProtocol> channel, ClusterNodeId localNode) {
        this.to = to;
        this.channel = channel;
        this.localNode = localNode;
    }

    public ClusterNodeId to() {
        return to;
    }

    public void send(Collection<MetricsReplica> replicas) {
        List<MetricsUpdate> updates = new ArrayList<>(replicas.size());

        long targetVer = -1;

        for (MetricsReplica replica : replicas) {
            replica.lock();

            try {
                if (replica.node().id().equals(to)) {
                    targetVer = replica.version();

                    // Do not send metrics of the target node.
                    continue;
                }

                if (replica.metrics() != null && !replica.metrics().isEmpty()) {
                    ClusterNodeId nodeId = replica.node().id();

                    Long prevVer = sentVersions.get(nodeId);

                    long newVer = replica.version();

                    if (prevVer == null || prevVer < newVer) {
                        long updated = sentVersions.merge(replica.node().id(), newVer, Long::max);

                        if (updated == newVer) {
                            MetricsUpdate update = new MetricsUpdate(replica.node().id(), replica.version(), replica.metrics());

                            updates.add(update);
                        }
                    }
                }
            } finally {
                replica.unlock();
            }
        }

        if (!updates.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Sending metrics update [to={}, updates={}]", to, updates);
            }

            channel.send(new MetricsProtocol.UpdateRequest(localNode, targetVer, updates));
        }
    }

    public void update(MetricsReplica replica) {
        replica.lock();

        try {
            sentVersions.merge(replica.node().id(), replica.version(), Long::max);
        } finally {
            replica.unlock();
        }
    }
}
