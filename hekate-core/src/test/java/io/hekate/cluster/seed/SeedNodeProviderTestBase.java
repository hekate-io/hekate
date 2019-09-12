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

import io.hekate.HekateNodeTestBase;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class SeedNodeProviderTestBase<T extends SeedNodeProvider> extends HekateNodeTestBase {
    protected static final String CLUSTER_1 = UUID.randomUUID().toString();

    protected static final String CLUSTER_2 = UUID.randomUUID().toString();

    protected abstract T createProvider() throws Exception;

    @Test
    public void testStartStopDiscovery() throws Exception {
        SeedNodeProvider provider = createProvider();

        repeat(3, i -> {
            InetSocketAddress address = newSocketAddress(10001);

            provider.startDiscovery(CLUSTER_1, address);
            provider.stopDiscovery(CLUSTER_1, address);
        });
    }

    @Test
    public void testMultipleStopDiscovery() throws Exception {
        SeedNodeProvider provider = createProvider();

        InetSocketAddress address = newSocketAddress(10001);

        provider.startDiscovery(CLUSTER_1, address);

        provider.stopDiscovery(CLUSTER_1, address);
        provider.stopDiscovery(CLUSTER_1, address);
        provider.stopDiscovery(CLUSTER_1, address);
        provider.stopDiscovery(CLUSTER_1, address);
    }

    @Test
    public void testDiscovery() throws Exception {
        repeat(4, i -> {
            Map<InetSocketAddress, SeedNodeProvider> providers = new HashMap<>();

            try {
                int nodes = i + 1;

                for (int j = 0; j < nodes; j++) {
                    SeedNodeProvider provider = createProvider();

                    InetSocketAddress address = newSocketAddress(10000 + j);

                    providers.put(address, provider);

                    provider.startDiscovery(CLUSTER_1, address);
                }

                for (Map.Entry<InetSocketAddress, SeedNodeProvider> e : providers.entrySet()) {
                    SeedNodeProvider provider = e.getValue();

                    List<InetSocketAddress> seedNodes = new ArrayList<>(provider.findSeedNodes(CLUSTER_1));

                    seedNodes.remove(e.getKey());

                    assertEquals(nodes - 1, seedNodes.size());

                    List<InetSocketAddress> allButThis = providers.keySet().stream()
                        .filter(a -> !a.equals(e.getKey()))
                        .collect(toList());

                    assertTrue(seedNodes.containsAll(allButThis));
                }
            } finally {
                for (Map.Entry<InetSocketAddress, SeedNodeProvider> e : providers.entrySet()) {
                    e.getValue().stopDiscovery(CLUSTER_1, e.getKey());
                }
            }
        });
    }

    @Test
    public void testToString() throws Exception {
        SeedNodeProvider provider = createProvider();

        assertEquals(ToString.format(provider), provider.toString());
    }
}
