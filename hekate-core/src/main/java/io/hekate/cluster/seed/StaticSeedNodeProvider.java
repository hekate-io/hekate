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
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Seed node provider with a pre-configured static list of seed node addresses.
 *
 * <p>
 * This provider uses a fixed set of addresses that can be specified at construction time via {@link StaticSeedNodeProviderConfig}
 * configuration object and can't be changed at runtime. Addresses can be specified via
 * {@link StaticSeedNodeProviderConfig#setAddresses(List)} method that accepts a list of strings in a form of {@code <host>:<port>}
 * (f.e. {@code 192.168.39.41:10012}).
 * </p>
 *
 * <p>
 * Below is the configuration example:
 * ${source: cluster/seed/StaticSeedNodeProviderJavadocTest.java#configuration}
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class StaticSeedNodeProvider implements SeedNodeProvider {
    private final List<InetSocketAddress> addresses;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     *
     * @throws UnknownHostException If one of the configured addresses couldn't be resolved.
     */
    public StaticSeedNodeProvider(StaticSeedNodeProviderConfig cfg) throws UnknownHostException {
        ConfigCheck.get(StaticSeedNodeProvider.class).notNull(cfg, "configuration");

        this.addresses = parseAddresses(cfg.getAddresses());
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        return addresses;
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public long cleanupInterval() {
        return 0;
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    /**
     * Returns the unmodifiable list of seed node addresses (see {@link StaticSeedNodeProviderConfig#setAddresses(List)}).
     *
     * @return Unmodifiable list of seed node addresses.
     */
    public List<InetSocketAddress> getAddresses() {
        return addresses;
    }

    private List<InetSocketAddress> parseAddresses(Collection<String> addresses) throws UnknownHostException {
        if (addresses != null && !addresses.isEmpty()) {
            ArrayList<InetSocketAddress> result = new ArrayList<>();

            for (String address : addresses) {
                if (address != null) {
                    InetSocketAddress socketAddress = AddressUtils.parse(address, ConfigCheck.get(StaticSeedNodeProviderConfig.class));

                    if (socketAddress != null) {
                        result.add(socketAddress);
                    }
                }
            }

            result.trimToSize();

            return Collections.unmodifiableList(result);
        }

        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
