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

package io.hekate.javadoc.cluster;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProvider;
import io.hekate.cluster.seed.multicast.MulticastSeedNodeProviderConfig;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class ClusterServiceJavadocTest extends HekateNodeTestBase {
    @Test
    public void exampleClusterService() throws Exception {
        // Start:configure
        // Prepare service factory and configure some options.
        ClusterServiceFactory factory = new ClusterServiceFactory()
            .withGossipInterval(1000)
            .withSeedNodeProvider(new MulticastSeedNodeProvider(
                new MulticastSeedNodeProviderConfig()
                    .withGroup("224.1.2.12")
                    .withPort(45454)
                    .withInterval(200)
                    .withWaitTime(1000)
            ));

        // ...other options...

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();

        // Access the service.
        ClusterService cluster = hekate.cluster();
        // End:configure

        assertNotNull(cluster);

        try {
            // Start:cluster_event_listener
            hekate.cluster().addListener(event -> {
                switch (event.type()) {
                    case JOIN: {
                        ClusterJoinEvent join = event.asJoin();

                        System.out.println("Joined : " + join.topology());

                        break;
                    }
                    case CHANGE: {
                        ClusterChangeEvent change = event.asChange();

                        System.out.println("Topology change :" + change.topology());
                        System.out.println("      added nodes=" + change.added());
                        System.out.println("    removed nodes=" + change.removed());

                        break;
                    }
                    case LEAVE: {
                        ClusterLeaveEvent leave = event.asLeave();

                        System.out.println("Left : " + leave.topology());

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unsupported event type: " + event);
                    }
                }
            });
            // End:cluster_event_listener

            // Start:list_topology
            // Immutable snapshot of the current cluster topology.
            ClusterTopology topology = hekate.cluster().topology();

            System.out.println("   Local node: " + topology.localNode());
            System.out.println("    All nodes: " + topology.nodes());
            System.out.println(" Remote nodes: " + topology.remoteNodes());
            System.out.println("   Join order: " + topology.joinOrder());
            System.out.println("  Oldest node: " + topology.oldest());
            System.out.println("Youngest node: " + topology.youngest());
            // End:list_topology

            // Start:filter_topology
            // Immutable copy that contains only nodes with the specified role.
            ClusterTopology filtered = topology.filter(node -> node.hasRole("my_role"));
            // End:filter_topology

            assertNotNull(filtered);

        } finally {
            hekate.leave();
        }
    }
}
