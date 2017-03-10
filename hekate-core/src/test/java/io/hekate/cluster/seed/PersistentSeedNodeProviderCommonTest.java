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

package io.hekate.cluster.seed;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.core.HekateTestInstance;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetSocketAddress;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class PersistentSeedNodeProviderCommonTest<T extends SeedNodeProvider> extends SeedNodeProviderCommonTest<T> {
    @Test
    public void testGetCleanupInterval() throws Exception {
        SeedNodeProvider provider = createProvider();

        assertTrue(provider.getCleanupInterval() > 0);
    }

    @Test
    public void testRegisterUnregister() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address1 = newSocketAddress(10001);
        InetSocketAddress address2 = newSocketAddress(10002);
        InetSocketAddress address3 = newSocketAddress(10001);
        InetSocketAddress address4 = newSocketAddress(10002);

        repeat(3, i -> {
            // Register.
            provider.registerRemoteAddress(CLUSTER_1, address1);
            provider.registerRemoteAddress(CLUSTER_1, address2);
            provider.registerRemoteAddress(CLUSTER_2, address3);
            provider.registerRemoteAddress(CLUSTER_2, address4);

            List<InetSocketAddress> nodes = provider.getSeedNodes(CLUSTER_1);

            assertEquals(2, nodes.size());
            assertTrue(nodes.contains(address1));
            assertTrue(nodes.contains(address2));

            nodes = provider.getSeedNodes(CLUSTER_2);

            assertEquals(2, nodes.size());
            assertTrue(nodes.contains(address3));
            assertTrue(nodes.contains(address4));

            // Unregister.
            provider.unregisterRemoteAddress(CLUSTER_1, address1);
            provider.unregisterRemoteAddress(CLUSTER_1, address2);
            provider.unregisterRemoteAddress(CLUSTER_2, address3);
            provider.unregisterRemoteAddress(CLUSTER_2, address4);

            assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());
        });
    }

    @Test
    public void testRegisterUnregisterWrongCluster() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address1 = newSocketAddress(10001);
        InetSocketAddress address2 = newSocketAddress(10002);
        InetSocketAddress address3 = newSocketAddress(10003);
        InetSocketAddress address4 = newSocketAddress(10004);

        // Register.
        provider.registerRemoteAddress(CLUSTER_1, address1);
        provider.registerRemoteAddress(CLUSTER_1, address2);
        provider.registerRemoteAddress(CLUSTER_2, address3);
        provider.registerRemoteAddress(CLUSTER_2, address4);

        List<InetSocketAddress> nodes = provider.getSeedNodes(CLUSTER_1);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address1));
        assertTrue(nodes.contains(address2));

        nodes = provider.getSeedNodes(CLUSTER_2);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address3));
        assertTrue(nodes.contains(address4));

        // Unregister with wrong cluster names.
        provider.unregisterRemoteAddress(CLUSTER_2, address1);
        provider.unregisterRemoteAddress(CLUSTER_2, address2);
        provider.unregisterRemoteAddress(CLUSTER_1, address3);
        provider.unregisterRemoteAddress(CLUSTER_1, address4);

        nodes = provider.getSeedNodes(CLUSTER_1);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address1));
        assertTrue(nodes.contains(address2));

        nodes = provider.getSeedNodes(CLUSTER_2);

        assertEquals(2, nodes.size());
        assertTrue(nodes.contains(address3));
        assertTrue(nodes.contains(address4));

        // Unregister.
        provider.unregisterRemoteAddress(CLUSTER_1, address1);
        provider.unregisterRemoteAddress(CLUSTER_1, address2);
        provider.unregisterRemoteAddress(CLUSTER_2, address3);
        provider.unregisterRemoteAddress(CLUSTER_2, address4);

        assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testRegisterDuplicate() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address = newSocketAddress(10001);

        provider.registerRemoteAddress(CLUSTER_1, address);
        provider.registerRemoteAddress(CLUSTER_1, address);
        provider.registerRemoteAddress(CLUSTER_1, address);

        List<InetSocketAddress> nodes = provider.getSeedNodes(CLUSTER_1);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(address));

        provider.unregisterRemoteAddress(CLUSTER_1, address);

        assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testUnregisterUnknown() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address = newSocketAddress(10001);

        provider.unregisterRemoteAddress(CLUSTER_1, address);

        assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());

        provider.registerRemoteAddress(CLUSTER_1, address);

        List<InetSocketAddress> nodes = provider.getSeedNodes(CLUSTER_1);

        assertEquals(1, nodes.size());
        assertTrue(nodes.contains(address));

        provider.unregisterRemoteAddress(CLUSTER_1, address);

        assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());

        provider.unregisterRemoteAddress(CLUSTER_1, address);

        assertTrue(provider.getSeedNodes(CLUSTER_1).isEmpty());
    }

    @Test
    public void testCleanupWithInstances() throws Exception {
        SeedNodeProvider controlProvider = createProvider();

        repeat(3, i -> {
            SeedNodeProvider provider = createProvider();

            HekateTestInstance node = createInstance(b -> {
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

            assertTrue(controlProvider.getSeedNodes(CLUSTER_1).contains(node.getSocketAddress()));

            InetSocketAddress fake1 = newSocketAddress(15001);
            InetSocketAddress fake2 = newSocketAddress(15002);
            InetSocketAddress fake3 = newSocketAddress(15003);

            controlProvider.registerRemoteAddress(CLUSTER_1, fake1);
            controlProvider.registerRemoteAddress(CLUSTER_1, fake2);
            controlProvider.registerRemoteAddress(CLUSTER_1, fake3);

            sayTime("Awaiting for seed node status change", () -> {
                awaitForSeedNodeStatus(false, fake1, CLUSTER_1, controlProvider);
                awaitForSeedNodeStatus(false, fake2, CLUSTER_1, controlProvider);
                awaitForSeedNodeStatus(false, fake3, CLUSTER_1, controlProvider);
            });

            controlProvider.unregisterRemoteAddress(CLUSTER_1, node.getSocketAddress());

            awaitForSeedNodeStatus(true, node.getSocketAddress(), CLUSTER_1, controlProvider);
        });
    }

    private void awaitForSeedNodeStatus(boolean available, InetSocketAddress node, String cluster, SeedNodeProvider provider)
        throws Exception {
        busyWait("address being " + (available ? "registered" : "unregistered") + " [address=" + node + ']', () -> {
            List<InetSocketAddress> nodes = provider.getSeedNodes(cluster);

            return available && nodes.contains(node) || !available && !nodes.contains(node);
        });
    }
}
