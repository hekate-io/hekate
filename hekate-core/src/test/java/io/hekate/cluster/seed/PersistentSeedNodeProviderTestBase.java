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

package io.hekate.cluster.seed;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class PersistentSeedNodeProviderTestBase<T extends SeedNodeProvider> extends SeedNodeProviderTestBase<T> {
    @Test
    public void testGetCleanupInterval() throws Exception {
        SeedNodeProvider provider = createProvider();

        assertTrue(provider.cleanupInterval() > 0);
    }

    @Test
    public void testRegisterUnregister() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address1 = newSocketAddress();
        InetSocketAddress address2 = newSocketAddress();
        InetSocketAddress address3 = newSocketAddress();
        InetSocketAddress address4 = newSocketAddress();

        repeat(3, i -> {
            // Register.
            provider.registerRemote(CLUSTER_1, address1);
            provider.registerRemote(CLUSTER_1, address2);
            provider.registerRemote(CLUSTER_2, address3);
            provider.registerRemote(CLUSTER_2, address4);

            List<InetSocketAddress> nodes = provider.findSeedNodes(CLUSTER_1);

            assertEquals(2, nodes.size());
            assertTrue(nodes.contains(address1));
            assertTrue(nodes.contains(address2));

            nodes = provider.findSeedNodes(CLUSTER_2);

            assertEquals(2, nodes.size());
            assertTrue(nodes.contains(address3));
            assertTrue(nodes.contains(address4));

            // Unregister.
            provider.unregisterRemote(CLUSTER_1, address1);
            provider.unregisterRemote(CLUSTER_1, address2);
            provider.unregisterRemote(CLUSTER_2, address3);
            provider.unregisterRemote(CLUSTER_2, address4);

            assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
        });
    }

    @Test
    public void testRegisterUnregisterWrongCluster() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address1 = newSocketAddress();
        InetSocketAddress address2 = newSocketAddress();
        InetSocketAddress address3 = newSocketAddress();
        InetSocketAddress address4 = newSocketAddress();

        // Register.
        provider.registerRemote(CLUSTER_1, address1);
        provider.registerRemote(CLUSTER_1, address2);
        provider.registerRemote(CLUSTER_2, address3);
        provider.registerRemote(CLUSTER_2, address4);

        List<InetSocketAddress> nodes = provider.findSeedNodes(CLUSTER_1);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address1));
        assertTrue(nodes.contains(address2));

        nodes = provider.findSeedNodes(CLUSTER_2);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address3));
        assertTrue(nodes.contains(address4));

        // Unregister with wrong cluster names.
        provider.unregisterRemote(CLUSTER_2, address1);
        provider.unregisterRemote(CLUSTER_2, address2);
        provider.unregisterRemote(CLUSTER_1, address3);
        provider.unregisterRemote(CLUSTER_1, address4);

        nodes = provider.findSeedNodes(CLUSTER_1);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address1));
        assertTrue(nodes.contains(address2));

        nodes = provider.findSeedNodes(CLUSTER_2);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address3));
        assertTrue(nodes.contains(address4));

        // Unregister.
        provider.unregisterRemote(CLUSTER_1, address1);
        provider.unregisterRemote(CLUSTER_1, address2);
        provider.unregisterRemote(CLUSTER_2, address3);
        provider.unregisterRemote(CLUSTER_2, address4);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testRegisterDuplicate() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address = newSocketAddress();

        provider.registerRemote(CLUSTER_1, address);
        provider.registerRemote(CLUSTER_1, address);
        provider.registerRemote(CLUSTER_1, address);

        List<InetSocketAddress> nodes = provider.findSeedNodes(CLUSTER_1);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(address));

        provider.unregisterRemote(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testUnregisterUnknown() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address = newSocketAddress();

        provider.unregisterRemote(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());

        provider.registerRemote(CLUSTER_1, address);

        List<InetSocketAddress> nodes = provider.findSeedNodes(CLUSTER_1);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(address));

        provider.unregisterRemote(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());

        provider.unregisterRemote(CLUSTER_1, address);

        assertTrue(provider.findSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testCleanupWithNodes() throws Exception {
        SeedNodeProvider controlProvider = createProvider();

        repeat(3, i -> {
            SeedNodeProvider provider = createProvider();

            HekateTestNode node = createNode(b -> {
                b.setClusterName(CLUSTER_1);

                b.withService(ClusterServiceFactory.class, cluster ->
                    cluster.setSeedNodeProvider(provider)
                );

                b.withService(NetworkServiceFactory.class, net -> {
                    // Increase timeout since ping failure by timeout doesn't trigger seed address removal.
                    net.setConnectTimeout(3000);
                });
            });

            node.join();

            assertTrue(controlProvider.findSeedNodes(CLUSTER_1).contains(node.localNode().socket()));

            InetSocketAddress fake1 = newSocketAddress();
            InetSocketAddress fake2 = newSocketAddress();
            InetSocketAddress fake3 = newSocketAddress();

            controlProvider.registerRemote(CLUSTER_1, fake1);
            controlProvider.registerRemote(CLUSTER_1, fake2);
            controlProvider.registerRemote(CLUSTER_1, fake3);

            sayTime("Awaiting for seed node status change", () -> {
                awaitForSeedNodeStatus(false, fake1, CLUSTER_1, controlProvider);
                awaitForSeedNodeStatus(false, fake2, CLUSTER_1, controlProvider);
                awaitForSeedNodeStatus(false, fake3, CLUSTER_1, controlProvider);
            });

            controlProvider.unregisterRemote(CLUSTER_1, node.localNode().socket());

            awaitForSeedNodeStatus(true, node.localNode().socket(), CLUSTER_1, controlProvider);
        });
    }

    private void awaitForSeedNodeStatus(boolean available, InetSocketAddress node, String cluster, SeedNodeProvider provider)
        throws Exception {
        busyWait("address being " + (available ? "registered" : "unregistered") + " [address=" + node + ']', () -> {
            List<InetSocketAddress> nodes = provider.findSeedNodes(cluster);

            return (available && nodes.contains(node)) || (!available && !nodes.contains(node));
        });
    }
}
