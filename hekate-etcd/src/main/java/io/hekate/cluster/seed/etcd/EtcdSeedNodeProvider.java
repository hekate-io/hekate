/*
 * Copyright 2021 The Hekate Project
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

package io.hekate.cluster.seed.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.options.GetOption;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static io.hekate.core.internal.util.Utils.nullOrTrim;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

/**
 * Etcd-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provider uses <a href="https://github.com/etcd-io/etcd" target="_blank">Etcd</a> key-value store to keep track of active seed nodes.
 * </p>
 *
 * <p>
 * When this provider start it registers the local node's address using the following key structure:<br>
 * {@link EtcdSeedNodeProviderConfig#setBasePath(String) [base_path]}/
 * {@link ClusterServiceFactory#setNamespace(String) [namespace]}/
 * [node_address]
 * </p>
 *
 * <p>
 * In order to find existing seed nodes this provider scans for keys that has the following prefix:<br>
 * {@link EtcdSeedNodeProviderConfig#setBasePath(String) [base_path]}/
 * {@link ClusterServiceFactory#setNamespace(String) [namespace]}/
 * </p>
 *
 * <p>
 * Please see the documentation of {@link EtcdSeedNodeProviderConfig} class for more details about the available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class EtcdSeedNodeProvider implements SeedNodeProvider, ConfigReportSupport {
    private interface EtcdTask {
        void execute(KV client) throws HekateException;
    }

    private static final Logger log = LoggerFactory.getLogger(EtcdSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String basePath;

    private final int cleanupInterval;

    private final List<URI> endpoints;

    private final String username;

    @ToStringIgnore
    private final String password;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public EtcdSeedNodeProvider(EtcdSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(EtcdSeedNodeProviderConfig.class);

        check.notEmpty(cfg.getBasePath(), "base path");
        check.notNull(cfg.getEndpoints(), "endpoints");
        check.notEmpty(nullSafe(cfg.getEndpoints()).map(String::trim).filter(x -> !x.isEmpty()), "endpoints");

        this.username = nullOrTrim(cfg.getUsername());
        this.password = nullOrTrim(cfg.getPassword());
        this.cleanupInterval = cfg.getCleanupInterval();
        this.endpoints = unmodifiableList(nullSafe(cfg.getEndpoints())
            .map(String::trim)
            .filter(endpoint -> !endpoint.isEmpty())
            .map(endpoint -> {
                try {
                    return new URI(endpoint);
                } catch (URISyntaxException e) {
                    throw check.fail(e);
                }
            })
            .collect(toList())
        );

        String basePath = cfg.getBasePath().trim();

        if (basePath.endsWith("/")) {
            this.basePath = basePath.substring(0, basePath.length() - 1);
        } else {
            this.basePath = basePath;
        }
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("etcd", r -> {
            r.value("endpoints", endpoints);
            r.value("base-path", basePath);
            r.value("cleanup-interval", cleanupInterval);
        });
    }

    /**
     * Returns the base path for storing seed nodes information in Etcd.
     *
     * @return Base path for storing seed nodes information in Etcd.
     *
     * @see EtcdSeedNodeProviderConfig#setBasePath(String)
     */
    public String basePath() {
        return basePath;
    }

    /**
     * Returns an immutable list of Etcd endpoints.
     *
     * @return immutable list of Etcd endpoints.
     *
     * @see EtcdSeedNodeProviderConfig#setEndpoints(List)
     */
    public List<URI> endpoints() {
        return endpoints;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        String prefix = keyPrefix(namespace);

        if (DEBUG) {
            log.debug("Searching for seed nodes [key-prefix={}]", prefix);
        }

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        withEtcd(client -> {
            try {
                ByteSequence key = bytes(prefix);

                GetResponse response = client.get(key, GetOption.newBuilder().withPrefix(key).build()).get();

                response.getKvs().stream()
                    .map(it -> AddressUtils.fromFileName(it.getValue().toString(UTF_8), log))
                    .filter(Objects::nonNull)
                    .forEach(seedNodes::add);

                if (DEBUG) {
                    log.debug("Done searching for seed nodes [found={}]", seedNodes.size());
                }
            } catch (ExecutionException | InterruptedException e) {
                throw new HekateException("Failed to load seed nodes from Etcd [key-prefix=" + prefix + ']', e);
            }
        });

        return seedNodes;
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        if (log.isInfoEnabled()) {
            log.info("Starting discovery [namespace={}, {}]", namespace, ToString.formatProperties(this));
        }

        withEtcd(client ->
            doRegister(client, namespace, node, true)
        );
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        withEtcd(client ->
            doUnregister(client, namespace, node, true)
        );
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        withEtcd(client ->
            doRegister(client, namespace, node, false)
        );
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
        withEtcd(client ->
            doUnregister(client, namespace, node, false)
        );
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    private void doRegister(KV client, String namespace, InetSocketAddress node, boolean local) throws HekateException {
        try {
            String addressStr = AddressUtils.toFileName(node);
            String key = keyPrefix(namespace) + addressStr;

            if (log.isInfoEnabled()) {
                log.info("Registering {} seed node [key={}]", local ? "local" : "remote", key);
            }

            client.put(bytes(key), bytes(addressStr)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new HekateException("Failed to register seed node [namespace=" + namespace + ", node=" + node + ']', e);
        }
    }

    private void doUnregister(KV client, String namespace, InetSocketAddress node, boolean local) throws HekateException {
        String key = keyPrefix(namespace) + AddressUtils.toFileName(node);

        try {
            if (log.isInfoEnabled()) {
                log.info("Unregistering {} seed node [key={}]", local ? "local" : "remote", key);
            }

            client.delete(bytes(key)).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new HekateException("Failed to unregister seed node [namespace=" + namespace + ", node=" + node + ']', e);
        }
    }

    private void withEtcd(EtcdTask task) throws HekateException {
        try (
            Client client = buildEtcd();
            KV kv = client.getKVClient()
        ) {
            task.execute(kv);
        }
    }

    private Client buildEtcd() {
        ClientBuilder builder = Client.builder().endpoints(endpoints);

        if (username != null && password != null) {
            builder.user(bytes(username));
            builder.password(bytes(password));
        }

        return builder.build();
    }

    private String keyPrefix(String namespace) {
        return basePath + '/' + namespace + '/';
    }

    private static ByteSequence bytes(String str) {
        return ByteSequence.from(str, UTF_8);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
