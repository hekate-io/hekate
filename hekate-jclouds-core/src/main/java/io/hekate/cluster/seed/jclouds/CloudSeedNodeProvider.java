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

package io.hekate.cluster.seed.jclouds;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationScope;
import org.jclouds.http.HttpResponseException;
import org.jclouds.location.reference.LocationConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toSet;

/**
 * Cloud instance-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provider uses {@link ComputeService} to get meta-information about all instances that are running in the cloud. Addresses of those
 * instances will be used as seed nodes for {@link ClusterService} cluster.
 * </p>
 *
 * <p>
 * <b>Important:</b> all {@link Hekate} cluster nodes must be configured to run on the same {@link NetworkServiceFactory#setPort(int)
 * port}.
 * </p>
 *
 * <p>
 * Filtering of seed node instances can be done by specifying different configuration options in {@link CloudSeedNodeProviderConfig}.
 * For example:
 * </p>
 * <ul>
 * <li>
 * {@link CloudSeedNodeProviderConfig#setRegions(Set) filter by regions} - to scan instances only within a limited set of cloud regions
 * </li>
 * <li>
 * {@link CloudSeedNodeProviderConfig#setZones(Set) filter by zones} - to scan instances only within a limited set of availability zones
 * </li>
 * <li>
 * {@link CloudSeedNodeProviderConfig#setTags(Map) filter by tags} - to scan only those instances that have the specified set of tags
 * </li>
 * </ul>
 *
 * <p>
 * Please see the documentation of {@link CloudSeedNodeProviderConfig} class for more details about the available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class CloudSeedNodeProvider implements SeedNodeProvider {
    private interface ComputeTask<T> {
        T execute(ComputeServiceContext ctx) throws HekateException;
    }

    private static final Logger log = LoggerFactory.getLogger(CloudSeedNodeProvider.class);

    private final String provider;

    private final String endpoint;

    private final Properties properties;

    private final Set<String> regions;

    private final Set<String> zones;

    private final Map<String, String> tags;

    @ToStringIgnore
    private final CredentialsSupplier credentials;

    @ToStringIgnore
    private int port;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public CloudSeedNodeProvider(CloudSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(CloudSeedNodeProviderConfig.class);

        check.notNull(cfg.getProvider(), "provider");
        check.notNull(cfg.getCredentials(), "credentials");

        this.provider = cfg.getProvider();
        this.credentials = cfg.getCredentials();
        this.endpoint = cfg.getEndpoint();

        this.regions = StreamUtils.nullSafe(cfg.getRegions()).collect(toSet());
        this.zones = StreamUtils.nullSafe(cfg.getZones()).collect(toSet());

        Properties properties = cfg.buildBaseProperties();

        if (!regions.isEmpty()) {
            properties.setProperty(LocationConstants.PROPERTY_REGIONS, String.join(",", regions));
        }

        if (!zones.isEmpty()) {
            properties.setProperty(LocationConstants.PROPERTY_ZONES, String.join(",", zones));
        }

        if (cfg.getProperties() != null) {
            cfg.getProperties().forEach(properties::put);
        }

        this.properties = properties;

        this.tags = cfg.getTags() != null ? new HashMap<>(cfg.getTags()) : Collections.emptyMap();
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        port = node.getPort();
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        // No-op.
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        return withCompute(ctx -> {
            try {
                ComputeService compute = ctx.getComputeService();

                Set<String> hosts = compute.listNodesDetailsMatching(Objects::nonNull).stream()
                    .filter(node -> {
                        if (!acceptState(node)) {
                            return false;
                        }

                        Location location = node.getLocation();

                        if (!acceptLocation(location, regions, LocationScope.REGION)) {
                            return false;
                        }

                        if (!acceptLocation(location, zones, LocationScope.ZONE)) {
                            return false;
                        }

                        if (!tags.isEmpty()) {
                            Map<String, String> metaData = node.getUserMetadata();

                            if (metaData == null || metaData.isEmpty()) {
                                return false;
                            }

                            for (Map.Entry<String, String> e : tags.entrySet()) {
                                if (!Objects.equals(e.getValue(), metaData.get(e.getKey()))) {
                                    return false;
                                }
                            }
                        }

                        return true;
                    })
                    .flatMap(node -> node.getPrivateAddresses().stream())
                    .collect(toSet());

                List<InetSocketAddress> seedNodes = new ArrayList<>(hosts.size());

                for (String host : hosts) {
                    try {
                        InetAddress address = InetAddress.getByName(host);

                        seedNodes.add(new InetSocketAddress(address, port));
                    } catch (UnknownHostException e) {
                        log.warn("Failed to resolve cloud node address [host={}]", host, e);
                    }
                }

                return seedNodes;
            } catch (HttpResponseException e) {
                if (ErrorUtils.isCausedBy(IOException.class, e)) {
                    throw new HekateException("Cloud provider connection failure [provider=" + provider + ']', e);
                } else {
                    throw e;
                }
            }
        });
    }

    @Override
    public void suspendDiscovery() throws HekateException {
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

    // Package level for testing purposes.
    boolean acceptState(NodeMetadata node) {
        return node.getStatus() == NodeMetadata.Status.RUNNING;
    }

    private boolean acceptLocation(Location location, Set<String> names, LocationScope scope) {
        if (names.isEmpty()) {
            return true;
        }

        while (location != null) {
            if (location.getScope() == scope && names.contains(location.getId())) {
                return true;
            }

            location = location.getParent();
        }

        return false;
    }

    private <T> T withCompute(ComputeTask<T> task) throws HekateException {
        ContextBuilder builder = ContextBuilder.newBuilder(provider)
            .credentialsSupplier(credentials::get)
            .modules(ImmutableSet.<Module>of(new SLF4JLoggingModule()));

        if (!properties.isEmpty()) {
            builder.overrides(properties);
        }

        if (endpoint != null) {
            String trimmedEndpoint = endpoint.trim();

            if (!trimmedEndpoint.isEmpty()) {
                builder.endpoint(trimmedEndpoint);
            }
        }

        try (ComputeServiceContext ctx = builder.buildView(ComputeServiceContext.class)) {
            return task.execute(ctx);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
