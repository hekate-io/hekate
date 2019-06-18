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

package io.hekate.cluster.seed.consul;

import com.orbitz.consul.Consul;
import com.orbitz.consul.KeyValueClient;
import com.orbitz.consul.model.kv.Value;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.cluster.seed.consul.ConsulSeedNodeProviderConfig.SEPARATOR;

/**
 * Implementation of {@link SeedNodeProvider} interface with usage of Consul's kv store.
 *
 * <p>
 * This provider uses <a href="https://github.com/hashicorp/consul" target="_blank">Consul</a> key-value store to keep track of active seed
 * nodes.
 * </p>
 *
 * <p>
 * When this provider start it registers the local node's address using the following key structure:<br>
 * {@link ConsulSeedNodeProviderConfig#setBasePath(String) [base_path]}/
 * {@link HekateBootstrap#setClusterName(String) [cluster_name]}/
 * [node_address]
 * </p>
 *
 * <p>
 * In order to find existing seed nodes this provider scans for keys that has the following prefix:<br>
 * {@link ConsulSeedNodeProviderConfig#setBasePath(String) [base_path]}/
 * {@link HekateBootstrap#setClusterName(String) [cluster_name]}/
 * </p>
 *
 * <p>
 * Please see the documentation of {@link ConsulSeedNodeProviderConfig} class for more details about the available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class ConsulSeedNodeProvider implements SeedNodeProvider {
    private static final Logger log = LoggerFactory.getLogger(ConsulSeedNodeProvider.class);

    private final long cleanupInterval;

    private final URI url;

    private final String basePath;

    /**
     * Initializes new consul seed node provider.
     *
     * @param cfg config
     */
    public ConsulSeedNodeProvider(ConsulSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(ConsulSeedNodeProviderConfig.class);

        check.notNull(cfg.getUrl(), "url");
        check.notEmpty(cfg.getBasePath(), "base path");

        this.cleanupInterval = cfg.getCleanupInterval();
        this.basePath = cfg.getBasePath().replaceFirst("^/+", "");

        try {
            url = new URI(cfg.getUrl());
        } catch (URISyntaxException e) {
            throw check.fail(e);
        }

        check.notEmpty(basePath, "base path");
    }

    /**
     * Returns the base path for storing seed nodes information in Consul kv storage.
     *
     * @return Base path for storing seed nodes information in Consul.
     *
     * @see ConsulSeedNodeProviderConfig#setBasePath(String)
     */
    public String basePath() {
        return basePath;
    }

    /**
     * Return url path to Consul HTTP API.
     *
     * @return Consul HTTP API path.
     *
     * @see ConsulSeedNodeProviderConfig#setUrl(String)
     */
    public URI url() {
        return url;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        String clusterPath = makeClusterPath(cluster);

        log.debug("Searching for seed nodes [cluster-path={}]", clusterPath);

        return runFuncWithClient(kv -> kv
            .getValues(makeClusterPath(cluster))
            .stream()
            .map(Value::getKey)
            .filter(s -> s.startsWith(clusterPath))
            .map(s -> s.replaceFirst(clusterPath + SEPARATOR, ""))
            .map(s -> AddressUtils.fromFileName(s, log))
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        doRegister(cluster, node, true);
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        doUnregister(cluster, node, true);
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        doRegister(cluster, node, false);
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        doUnregister(cluster, node, false);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }

    private void doRegister(String cluster, InetSocketAddress node, boolean local) throws HekateException {
        String path = makePath(cluster, node);

        if (log.isInfoEnabled()) {
            log.info("Registering {} seed node [key={}]", local ? "local" : "remote", path);
        }

        runWithClient(kv -> kv.putValue(path));
    }

    private void doUnregister(String cluster, InetSocketAddress node, boolean local) throws HekateException {
        String path = makePath(cluster, node);

        if (log.isInfoEnabled()) {
            log.info("Unregistering {} seed node [key={}]", local ? "local" : "remote", path);
        }

        runWithClient(kv -> kv.deleteKey(path));
    }

    private String makeClusterPath(String cluster) {
        return basePath + SEPARATOR + cluster;
    }

    private String makePath(String cluster, InetSocketAddress node) {
        String addressStr = AddressUtils.toFileName(node);

        return makeClusterPath(cluster) + SEPARATOR + addressStr;
    }

    private <T> T runFuncWithClient(Function<KeyValueClient, T> f) {
        Consul consul = Consul.builder().withUrl(url.toString()).build();

        KeyValueClient kvClient = consul.keyValueClient();

        try {
            return f.apply(kvClient);
        } finally {
            consul.destroy();
        }
    }

    private void runWithClient(Consumer<KeyValueClient> consumer) {
        Consul consul = Consul.builder().withUrl(url.toString()).build();

        KeyValueClient kvClient = consul.keyValueClient();

        try {
            consumer.accept(kvClient);
        } finally {
            consul.destroy();
        }
    }
}
