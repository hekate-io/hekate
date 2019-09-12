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
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

/**
 * Implementation of {@link SeedNodeProvider} interface with usage of Consul's Key-Value store.
 *
 * <p>
 * This provider uses <a href="https://github.com/hashicorp/consul" target="_blank">Consul</a> Key-Value store to keep track of active seed
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
public class ConsulSeedNodeProvider implements SeedNodeProvider, ConfigReportSupport {
    private static final Logger log = LoggerFactory.getLogger(ConsulSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final Pattern LEADING_OR_TRAILING_SLASHES = Pattern.compile("^(/+)|(/+$)");

    private final URI url;

    private final String basePath;

    private final long cleanupInterval;

    private final Long connectTimeout;

    private final Long readTimeout;

    private final Long writeTimeout;

    /**
     * Initializes new consul seed node provider.
     *
     * @param cfg Configuration.
     */
    public ConsulSeedNodeProvider(ConsulSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(ConsulSeedNodeProviderConfig.class);

        check.notNull(cfg.getUrl(), "url");
        check.notEmpty(cfg.getBasePath(), "base path");

        this.basePath = normalizeBasePath(cfg.getBasePath());
        this.cleanupInterval = cfg.getCleanupInterval();
        this.connectTimeout = cfg.getConnectTimeout();
        this.readTimeout = cfg.getReadTimeout();
        this.writeTimeout = cfg.getWriteTimeout();

        try {
            url = new URI(cfg.getUrl());
        } catch (URISyntaxException e) {
            throw check.fail(e);
        }
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("consul", r -> {
            r.value("url", url);
            r.value("base-path", basePath);
            r.value("cleanup-interval", cleanupInterval);
            r.value("connect-timeout", connectTimeout);
            r.value("read-timeout", readTimeout);
            r.value("write-timeout", writeTimeout);
        });
    }

    /**
     * Returns the base path for storing seed nodes information in Consul Key-Value storage.
     *
     * @return Base path for storing seed nodes information in Consul.
     *
     * @see ConsulSeedNodeProviderConfig#setBasePath(String)
     */
    public String basePath() {
        return basePath;
    }

    /**
     * Return URL path to Consul HTTP API.
     *
     * @return Consul HTTP API path.
     *
     * @see ConsulSeedNodeProviderConfig#setUrl(String)
     */
    public URI url() {
        return url;
    }

    /**
     * Returns the Consul connect timeout in milliseconds.
     *
     * @return Connect timeout in milliseconds.
     */
    public Long connectTimeout() {
        return connectTimeout;
    }

    /**
     * Returns the Consul read timeout in milliseconds.
     *
     * @return Read timeout in milliseconds.
     */
    public Long readTimeout() {
        return readTimeout;
    }

    /**
     * Returns the Consul write timeout in milliseconds.
     *
     * @return Write timeout in milliseconds.
     */
    public Long writeTimeout() {
        return writeTimeout;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        String clusterPath = makeClusterPath(cluster);

        if (DEBUG) {
            log.debug("Searching for seed nodes [cluster-path={}]", clusterPath);
        }

        return getWithClient(consul ->
            consul.getValues(clusterPath).stream()
                .map(Value::getKey)
                .filter(it -> it.startsWith(clusterPath) && it.length() > clusterPath.length())
                .map(it -> it.substring(clusterPath.length()))
                .map(it -> AddressUtils.fromFileName(it, log))
                .filter(Objects::nonNull)
                .collect(toList())
        );
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

    private void doRegister(String cluster, InetSocketAddress node, boolean local) throws HekateException {
        String path = makeAddressPath(cluster, node);

        if (log.isInfoEnabled()) {
            log.info("Registering {} seed node [key={}]", local ? "local" : "remote", path);
        }

        withConsul(consul ->
            consul.putValue(path)
        );
    }

    private void doUnregister(String cluster, InetSocketAddress node, boolean local) throws HekateException {
        String path = makeAddressPath(cluster, node);

        if (log.isInfoEnabled()) {
            log.info("Unregistering {} seed node [key={}]", local ? "local" : "remote", path);
        }

        withConsul(consul ->
            consul.deleteKey(path)
        );
    }

    private String makeAddressPath(String cluster, InetSocketAddress node) {
        return makeClusterPath(cluster) + AddressUtils.toFileName(node);
    }

    private String makeClusterPath(String cluster) {
        return basePath + '/' + cluster + '/';
    }

    private <T> T getWithClient(Function<KeyValueClient, T> f) {
        Consul.Builder builder = Consul.builder().withUrl(url.toString());

        if (connectTimeout != null) {
            builder.withConnectTimeoutMillis(connectTimeout);
        }

        if (readTimeout != null) {
            builder.withReadTimeoutMillis(readTimeout);
        }

        if (writeTimeout != null) {
            builder.withWriteTimeoutMillis(writeTimeout);
        }

        Consul consul = builder.build();

        try {
            return f.apply(consul.keyValueClient());
        } finally {
            consul.destroy();
        }
    }

    private void withConsul(Consumer<KeyValueClient> consumer) {
        Consul consul = Consul.builder().withUrl(url.toString()).build();

        try {
            consumer.accept(consul.keyValueClient());
        } finally {
            consul.destroy();
        }
    }

    private static String normalizeBasePath(String basePath) {
        return LEADING_OR_TRAILING_SLASHES.matcher(basePath.trim()).replaceAll("");
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
