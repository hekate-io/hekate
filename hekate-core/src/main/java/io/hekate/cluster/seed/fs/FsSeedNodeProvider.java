/*
 * Copyright 2022 The Hekate Project
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

package io.hekate.cluster.seed.fs;

import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.report.ConfigReportSupport;
import io.hekate.core.report.ConfigReporter;
import io.hekate.util.format.ToString;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * File system-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provider uses a shared folder to store seed node addresses. When provider starts discovering other nodes it creates a new empty
 * file whose name contains local node's host address and port in the
 * {@link FsSeedNodeProviderConfig#setWorkDir(File) [work_dir]}/{@link ClusterServiceFactory#setNamespace(String) [cluster_namespace]}
 * folder. In order to find other seed nodes it reads the list of all files in that folder and parses addresses from their names.
 * </p>
 *
 * <p>
 * Note that typically {@link FsSeedNodeProviderConfig#setWorkDir(File) [work_dir]} should be placed on a distributed file system.
 * Otherwise only those nodes that are placed on the same host will be able to discover each other.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link FsSeedNodeProviderConfig} class for more details about the available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class FsSeedNodeProvider implements SeedNodeProvider, JmxSupport<FsSeedNodeProviderJmx>, ConfigReportSupport {
    private static final Logger log = LoggerFactory.getLogger(FsSeedNodeProvider.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final FileFilter FILE_FILTER = pathname -> pathname.isFile() && pathname.getName().startsWith(AddressUtils.FILE_PREFIX);

    private final File workDir;

    private final long cleanupInterval;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     *
     * @throws IOException If failed to resolve {@link File#getCanonicalFile() canonical path} to
     * {@link FsSeedNodeProviderConfig#getWorkDir()}.
     */
    public FsSeedNodeProvider(FsSeedNodeProviderConfig cfg) throws IOException {
        ArgAssert.notNull(cfg, "configuration");

        ConfigCheck check = ConfigCheck.get(FsSeedNodeProviderConfig.class);

        File dir = cfg.getWorkDir();

        check.notNull(dir, "work directory");

        dir = dir.getCanonicalFile();

        if (dir.exists()) {
            check.isTrue(dir.isDirectory(), "work directory is not a directory [path=" + dir + ']');
            check.isTrue(dir.canRead(), "work directory is not readable [path=" + dir + ']');
            check.isTrue(dir.canWrite(), "work directory is not writable [path=" + dir + ']');
        }

        this.cleanupInterval = cfg.getCleanupInterval();
        this.workDir = dir;
    }

    @Override
    public void report(ConfigReporter report) {
        report.section("shared-folder", r -> {
            r.value("work-dir", workDir);
            r.value("cleanup-interval", cleanupInterval);
        });
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String namespace) throws HekateException {
        List<InetSocketAddress> seedNodes = new ArrayList<>();

        File dir = clusterDir(namespace);

        if (DEBUG) {
            log.debug("Searching for seed node files [dir={}]", dir);
        }

        File[] files = dir.listFiles();

        if (files != null) {
            for (File file : files) {
                if (FILE_FILTER.accept(file)) {
                    InetSocketAddress address = AddressUtils.fromFileName(file.getName(), log);

                    if (address != null) {
                        if (DEBUG) {
                            log.debug("Seed node address discovered [address={}]", address);
                        }

                        seedNodes.add(address);
                    }
                }
            }
        }

        if (DEBUG) {
            log.debug("Done searching for seed node files [found={}]", seedNodes.size());
        }

        return seedNodes;
    }

    @Override
    public void startDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        log.info("Starting discovery [namespace={}, {}]", namespace, ToString.formatProperties(this));

        doRegister(namespace, node);
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    @Override
    public void stopDiscovery(String namespace, InetSocketAddress node) throws HekateException {
        log.info("Stopping seed nodes discovery [namespace={}, address={}]", namespace, node);

        doUnregister(namespace, node);
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    @Override
    public void registerRemote(String namespace, InetSocketAddress node) throws HekateException {
        if (DEBUG) {
            log.debug("Registering remote address [namespace={}], node={}]", namespace, node);
        }

        doRegister(namespace, node);
    }

    @Override
    public void unregisterRemote(String namespace, InetSocketAddress node) throws HekateException {
        if (DEBUG) {
            log.debug("Unregistering remote address [namespace={}], node={}]", namespace, node);
        }

        doUnregister(namespace, node);
    }

    /**
     * Returns the work directory where seed node addresses are stored (see {@link FsSeedNodeProviderConfig#setWorkDir(File)}).
     *
     * @return The work directory where seed node addresses are stored.
     */
    public File getWorkDir() {
        return workDir;
    }

    @Override
    public FsSeedNodeProviderJmx jmx() {
        return new FsSeedNodeProviderJmx() {
            @Override
            public String getWorkDir() {
                return workDir.getAbsolutePath();
            }

            @Override
            public long getCleanupInterval() {
                return cleanupInterval;
            }
        };
    }

    private void doRegister(String cluster, InetSocketAddress node) throws HekateException {
        File dir = clusterDir(cluster);

        if (dir.mkdirs()) {
            log.info("Initialized directories structure for seed nodes store [path={}]", dir.getAbsolutePath());
        }

        File seedFile = new File(dir, AddressUtils.toFileName(node));

        log.info("Creating seed node file [path={}]", seedFile);

        try {
            if (seedFile.createNewFile()) {
                log.info("Created new seed node file [file={}]", seedFile);
            } else {
                log.info("Seed node file already exists [file={}]", seedFile);
            }
        } catch (IOException e) {
            throw new HekateException("Failed to create file for seed node [node=" + node + ", file=" + seedFile + ']', e);
        }
    }

    private void doUnregister(String cluster, InetSocketAddress node) {
        File dir = clusterDir(cluster);

        File seedFile = new File(dir, AddressUtils.toFileName(node));

        log.info("Deleting seed node file [path={}]", seedFile);

        if (seedFile.exists() && seedFile.isFile()) {
            if (seedFile.delete()) {
                log.info("Successfully deleted seed node file [path={}]", seedFile);
            } else {
                log.warn("Couldn't delete seed node file [path={}]", seedFile);
            }
        } else {
            log.info("Files doesn't exist or is not a file [path={}]", seedFile);
        }
    }

    // Package level for testing purposes.
    File clusterDir(String cluster) {
        return new File(workDir.getAbsolutePath(), cluster).getAbsoluteFile();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
