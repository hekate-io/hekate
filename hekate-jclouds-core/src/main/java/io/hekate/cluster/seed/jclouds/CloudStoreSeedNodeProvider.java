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
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.ContainerNotFoundException;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.domain.StorageType;
import org.jclouds.blobstore.options.ListContainerOptions;
import org.jclouds.http.HttpResponseException;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.rest.ResourceNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cloud store-based implementation of {@link SeedNodeProvider} interface.
 *
 * <p>
 * This provider uses a cloud storage (f.e. Amazon S3) to keep track of active seed nodes. When provider starts discovering other nodes it
 * uses {@link BlobStore} to creates a new empty blob whose name contains local node's host address and port. Such blob is stored in a
 * {@link CloudStoreSeedNodeProviderConfig#setContainer(String) configurable} container (aka bucket) under
 * /{@link HekateBootstrap#setClusterName(String) [cluster_name]}/ folder. In order to find other seed nodes it reads the list of all blobs
 * in that folder and parses addresses from their names.
 * </p>
 *
 * <p>
 * Please see the documentation of {@link CloudStoreSeedNodeProviderConfig} class for details about the available configuration options.
 * </p>
 *
 * @see ClusterServiceFactory#setSeedNodeProvider(SeedNodeProvider)
 * @see SeedNodeProvider
 */
public class CloudStoreSeedNodeProvider implements SeedNodeProvider {
    private static final Logger log = LoggerFactory.getLogger(CloudStoreSeedNodeProvider.class);

    private final String provider;

    private final String container;

    private final Properties properties;

    private final long cleanupInterval;

    @ToStringIgnore
    private final CredentialsSupplier credentials;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public CloudStoreSeedNodeProvider(CloudStoreSeedNodeProviderConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(CloudStoreSeedNodeProvider.class);

        check.notNull(cfg.getProvider(), "provider");
        check.notNull(cfg.getCredentials(), "credentials");
        check.notEmpty(cfg.getContainer(), "container");

        this.provider = cfg.getProvider();
        this.container = cfg.getContainer().trim();
        this.credentials = cfg.getCredentials();
        this.cleanupInterval = cfg.getCleanupInterval();

        Properties properties = cfg.buildBaseProperties();

        if (cfg.getProperties() != null) {
            cfg.getProperties().forEach(properties::put);
        }

        this.properties = properties;
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        registerAddress(cluster, node);
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        unregisterAddress(cluster, node);
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        try {
            if (log.isDebugEnabled()) {
                log.debug("Loading seed node addresses [container={}, cluster={}]", container, cluster);
            }

            try (BlobStoreContext ctx = createContext()) {
                BlobStore store = ctx.getBlobStore();

                List<InetSocketAddress> seedNodes = new ArrayList<>();

                String marker = null;

                do {
                    ListContainerOptions opts = ListContainerOptions.Builder.prefix(cluster + "/");

                    if (marker != null) {
                        opts.afterMarker(marker);
                    }

                    PageSet<? extends StorageMetadata> pageSet = store.list(container, opts);

                    if (log.isDebugEnabled()) {
                        log.debug("Loaded blobs list [size={}, marker={}]", pageSet.size(), marker);
                    }

                    for (StorageMetadata metaData : pageSet) {
                        if (metaData.getType() == StorageType.BLOB) {
                            String name = metaData.getName();

                            // Remove cluster prefix from the blob name.
                            if ((name.startsWith(cluster + '/') || name.startsWith(cluster + '\\'))
                                && name.length() > cluster.length() + 1) {
                                name = name.substring(cluster.length() + 1);
                            }

                            // Remove trailing '/' from the blob name.
                            if (name.length() > 1) {
                                int lastCharIdx = name.length() - 1;

                                if (name.charAt(lastCharIdx) == '/' || name.charAt(lastCharIdx) == '\\') {
                                    name = name.substring(0, lastCharIdx);
                                }
                            }

                            if (log.isDebugEnabled()) {
                                log.debug("Processing blob [name={}]", name);
                            }

                            InetSocketAddress address = AddressUtils.fromFileName(name, log);

                            if (address != null) {
                                seedNodes.add(address);
                            }
                        }
                    }

                    marker = pageSet.getNextMarker();
                } while (marker != null);

                return seedNodes;
            }
        } catch (ContainerNotFoundException e) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to load seed nodes list [container={}, cluster={}, cause={}]", container, cluster, e.toString());
            }

            return Collections.emptyList();
        } catch (HttpResponseException e) {
            if (ErrorUtils.isCausedBy(IOException.class, e)) {
                throw new HekateException("Cloud provider connection failure [provider=" + provider + ']', e);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        registerAddress(cluster, node);
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        unregisterAddress(cluster, node);
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        // No-op.
    }

    @Override
    public long cleanupInterval() {
        return cleanupInterval;
    }

    private BlobStoreContext createContext() {
        ContextBuilder builder = ContextBuilder.newBuilder(provider)
            .credentialsSupplier(credentials::get)
            .modules(ImmutableSet.<Module>of(new SLF4JLoggingModule()));

        if (!properties.isEmpty()) {
            builder.overrides(properties);
        }

        return builder.buildView(BlobStoreContext.class);
    }

    private void registerAddress(String cluster, InetSocketAddress address) throws HekateException {
        try (BlobStoreContext ctx = createContext()) {
            BlobStore store = ctx.getBlobStore();

            String file = cluster + '/' + AddressUtils.toFileName(address);

            try {
                if (!store.blobExists(container, file)) {
                    Blob blob = store.blobBuilder(file)
                        .type(StorageType.BLOB)
                        .payload(new byte[]{1})
                        .build();

                    store.putBlob(container, blob);

                    if (log.isInfoEnabled()) {
                        log.info("Registered address to the cloud store [container={}, file={}]", container, file);
                    }
                }
            } catch (ResourceNotFoundException | HttpResponseException e) {
                throw new HekateException("Failed to register the seed node address to the cloud store "
                    + "[container=" + container + ", file=" + file + ']', e);
            }
        }
    }

    private void unregisterAddress(String cluster, InetSocketAddress address) {
        try (BlobStoreContext ctx = createContext()) {
            BlobStore store = ctx.getBlobStore();

            String file = cluster + '/' + AddressUtils.toFileName(address);

            try {
                if (store.blobExists(container, file)) {
                    store.removeBlob(container, file);

                    if (log.isInfoEnabled()) {
                        log.info("Unregistered address from the cloud store [container={}, file={}]", container, file);
                    }
                }
            } catch (ResourceNotFoundException | HttpResponseException e) {
                if (log.isWarnEnabled()) {
                    log.warn("Failed to unregister the seed node address from the cloud store "
                        + "[container={}, file={}, cause={}]", container, file, e.toString());
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
