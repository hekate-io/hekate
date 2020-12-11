/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.cluster.seed.zookeeper;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZooKeeper-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provides uses a ZooKeeper cluster to store and discover seed node addresses. When provider starts discovering other nodes it creates
 * a new empty Z-node whose name contains local node's host address and under the {@link ZooKeeperSeedNodeProviderConfig#setBasePath(String)
 * [base_path]}/{@link ClusterServiceFactory#setNamespace(String) [namespace]} path. In order to find other seed nodes it reads the list of
 * all Z-nodes in that folder and parses addresses from their names.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link ZooKeeperSeedNodeProviderConfig} class for more details about the available configuration
 * options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class ZooKeeperSeedNodeProvider implements SeedNodeProvider, ConfigReportSupport {
    private interface ZooKeeperTask {
        void execute(CuratorFramework client) throws HekateException;
    }

    private static final Logger log = LoggerFactory.getLogger(ZooKeeperSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String connectionString;

    private final String basePath;

    private final int connectTimeout;

    private final int sessionTimeout;

    private final int cleanupInterval;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public ZooKeeperSeedNodeProvider(ZooKeeperSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(ZooKeeperSeedNodeProviderConfig.class);

        check.notEmpty(cfg.getConnectionString(), "connection string");
        check.notEmpty(cfg.getBasePath(), "base path");
        check.positive(cfg.getConnectTimeout(), "connect timeout");
        check.positive(cfg.getSessionTimeout(), "session timeout");

        connectionString = cfg.getConnectionString().trim();
        connectTimeout = cfg.getConnectTimeout();
        sessionTimeout = cfg.getSessionTimeout();
        cleanupInterval = cfg.getCleanupInterval();

        String path = cfg.getBasePath().trim();

        if (path.endsWith("/")) {
            basePath = path;
        } else {
            basePath = path + '/';
        }
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("zookeeper", r -> {
            r.value("connection-string", connectionString);
            r.value("base-path", basePath);
            r.value("connect-timeout", connectTimeout);
            r.value("session-timeout", sessionTimeout);
            r.value("cleanup-interval", cleanupInterval);
        });
    }

    /**
     * Returns the ZooKeeper connection string.
     *
     * @return ZooKeeper connection string.
     *
     * @see ZooKeeperSeedNodeProviderConfig#setConnectionString(String)
     */
    public String connectionString() {
        return connectionString;
    }

    /**
     * Returns the base path for storing seed nodes information in ZooKeeper.
     *
     * @return Base path for storing seed nodes information in ZooKeeper.
     *
     * @see ZooKeeperSeedNodeProviderConfig#setBasePath(String)
     */
    public String basePath() {
        return basePath;
    }

    /**
     * Zookeeper connect timeout.
     *
     * @return Zookeeper connect timeout.
     *
     * @see ZooKeeperSeedNodeProviderConfig#setConnectTimeout(int)
     */
    public int connectTimeout() {
        return connectTimeout;
    }

    /**
     * Zookeeper session timeout.
     *
     * @return Zookeeper session timeout.
     *
     * @see ZooKeeperSeedNodeProviderConfig#setSessionTimeout(int)
     */
    public int sessionTimeout() {
        return sessionTimeout;
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        String path = basePath + namespace;

        if (DEBUG) {
            log.debug("Searching for seed nodes [path={}]", path);
        }

        List<InetSocketAddress> seedNodes = new ArrayList<>();

        withZooKeeper(client -> {
            try {
                client.getChildren()
                    .forPath(path).stream()
                    .map(it -> AddressUtils.fromFileName(it, log))
                    .filter(Objects::nonNull)
                    .peek(address -> {
                        if (DEBUG) {
                            log.debug("Seed node address discovered [address={}]", address);
                        }
                    })
                    .forEach(seedNodes::add);

                if (DEBUG) {
                    log.debug("Done searching for seed nodes [found={}]", seedNodes.size());
                }
            } catch (NoNodeException e) {
                // No-op.
            } catch (Exception e) {
                throw new HekateException("Failed to load seed nodes from ZooKeeper [path=" + path + ']', e);
            }
        });

        return seedNodes;
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        if (log.isInfoEnabled()) {
            log.info("Starting discovery [namespace={}, {}]", namespace, ToString.formatProperties(this));
        }

        withZooKeeper(client ->
            doRegister(client, namespace, node, true)
        );
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        withZooKeeper(client ->
            doUnregister(client, namespace, node, true)
        );
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        withZooKeeper(client ->
            doRegister(client, namespace, node, false)
        );
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
        withZooKeeper(client ->
            doUnregister(client, namespace, node, false)
        );
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    private void doRegister(CuratorFramework client, String namespace, InetSocketAddress node, boolean local) throws HekateException {
        try {
            String clusterDir = basePath + namespace;

            String seedPath = clusterDir + '/' + AddressUtils.toFileName(node);

            if (log.isInfoEnabled()) {
                log.info("Registering {} seed node [path={}]", local ? "local" : "remote", seedPath);
            }

            createDirs(client, clusterDir);

            // Create a new z-node.
            client.create().withMode(CreateMode.PERSISTENT).forPath(seedPath);
        } catch (NodeExistsException e) {
            // Ignore (node already registered).
        } catch (Exception e) {
            throw new HekateException("Failed to register seed node to ZooKeeper [namespace=" + namespace + ", node=" + node + ']', e);
        }
    }

    private void doUnregister(CuratorFramework client, String namespace, InetSocketAddress node, boolean local) throws HekateException {
        String seedPath = basePath + namespace + '/' + AddressUtils.toFileName(node);

        try {
            if (log.isInfoEnabled()) {
                log.info("Unregistering {} seed node [path={}]", local ? "local" : "remote", seedPath);
            }

            client.delete().forPath(seedPath);
        } catch (NoNodeException e) {
            // Ignore.
        } catch (Exception e) {
            throw new HekateException("Failed to unregister seed node from ZooKeeper [namespace=" + namespace + ", node=" + node + ']', e);
        }
    }

    private void createDirs(CuratorFramework client, String dir) throws Exception {
        StringBuilder path = new StringBuilder();

        for (String name : dir.split("/", -1)) {
            if (!name.isEmpty()) {
                path.append('/').append(name);

                try {
                    client.create().withMode(CreateMode.PERSISTENT).forPath(path.toString());

                    if (DEBUG) {
                        log.debug("Created a base directory for seed nodes [path={}]", path);
                    }
                } catch (NodeExistsException e) {
                    // Ignore (path already exists).
                }
            }
        }
    }

    private void withZooKeeper(ZooKeeperTask task) throws HekateException {
        try (CuratorFramework client = CuratorFrameworkFactory.newClient(
            connectionString,
            sessionTimeout,
            connectTimeout,
            new RetryOneTime(0)
        )) {
            client.start();

            try {
                if (!client.blockUntilConnected(connectTimeout, TimeUnit.MILLISECONDS)) {
                    throw new HekateException("Timeout connecting to ZooKeeper [connections-string=" + connectionString + ']');
                }
            } catch (InterruptedException e) {
                throw new HekateException("Thread got interrupted while connecting to ZooKeeper.", e);
            }

            task.execute(client);
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
