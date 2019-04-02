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

import io.hekate.cluster.ClusterService;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.internal.util.StreamUtils.nullSafe;
import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

/**
 * Group of {@link SeedNodeProvider}s.
 *
 * <p>
 * This class provides support for registering multiple {@link SeedNodeProvider}s simultaneously to the {@link ClusterService} and act as a
 * single provider.
 * </p>
 *
 * <p>
 * It is possible to specify an error handling policy via the {@link SeedNodeProviderGroupConfig#setPolicy(SeedNodeProviderGroupPolicy)}
 * method. For the list of available policies please see the documentation of {@link SeedNodeProviderGroupPolicy} enum's values.
 * </p>
 *
 * <p>
 * Below is the configuration example:
 * ${source: cluster/seed/SeedNodeProviderGroupJavadocTest.java#configuration}
 * </p>
 *
 * @see SeedNodeProviderGroupPolicy
 * @see SeedNodeProviderGroupConfig
 */
public class SeedNodeProviderGroup implements SeedNodeProvider, JmxSupport<Collection<? extends SeedNodeProvider>> {
    /** See {@link #withPolicy(String, List, SeedNodeProviderTask)}. */
    private interface SeedNodeProviderTask {
        void execute(SeedNodeProvider provider) throws HekateException;
    }

    private static final Logger log = LoggerFactory.getLogger(SeedNodeProviderGroup.class);

    /** Error handling policy (see {@link #withPolicy(String, List, SeedNodeProviderTask)}. */
    private final SeedNodeProviderGroupPolicy policy;

    /** All configured providers. */
    private final List<SeedNodeProvider> allProviders;

    /** Providers that were successfully started in {@link #startDiscovery(String, InetSocketAddress)}. */
    private final List<SeedNodeProvider> liveProviders = new CopyOnWriteArrayList<>();

    /**
     * Constructs a new instance.
     *
     * @param cfg Configuration.
     */
    public SeedNodeProviderGroup(SeedNodeProviderGroupConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        this.policy = cfg.getPolicy();
        this.allProviders = unmodifiableList(nullSafe(cfg.getProviders()).collect(toList()));

        ConfigCheck check = ConfigCheck.get(SeedNodeProviderGroupConfig.class);

        check.notNull(policy, "policy");
        check.isFalse(allProviders.isEmpty(), "providers can't be empty.");
    }

    /**
     * Returns an immutable list of all providers that are configured for this group.
     *
     * @return Immutable list of all providers of this group.
     *
     * @see SeedNodeProviderGroupConfig#setProviders(List)
     */
    public List<SeedNodeProvider> allProviders() {
        return allProviders;
    }

    /**
     * Returns a copy of a list of all providers that were successfully started.
     *
     * @return Copy of a list of all providers that were successfully started.
     *
     * @see SeedNodeProvider#startDiscovery(String, InetSocketAddress)
     */
    public List<SeedNodeProvider> liveProviders() {
        return new ArrayList<>(liveProviders);
    }

    /**
     * Searches for a seed node provider of the specified type.
     *
     * @param type Type of a seed node provider.
     * @param <T> Type of a seed node provider.
     *
     * @return Seed node provider.
     */
    public <T> Optional<T> findProvider(Class<T> type) {
        return allProviders.stream()
            .filter(it -> type.isAssignableFrom(it.getClass()))
            .map(type::cast)
            .findFirst();
    }

    /**
     * Returns the error handling policy of this group.
     *
     * @return Error handling policy of this group.
     *
     * @see SeedNodeProviderGroupConfig#setPolicy(SeedNodeProviderGroupPolicy)
     */
    public SeedNodeProviderGroupPolicy policy() {
        return policy;
    }

    @Override
    public long cleanupInterval() {
        return allProviders.stream()
            .mapToLong(SeedNodeProvider::cleanupInterval)
            .filter(i -> i > 0)
            .min()
            .orElse(0);
    }

    @Override
    public List<InetSocketAddress> findSeedNodes(String cluster) throws HekateException {
        List<InetSocketAddress> seedNodes = new ArrayList<>();

        withPolicy("find seed nodes", liveProviders, provider ->
            seedNodes.addAll(provider.findSeedNodes(cluster))
        );

        return seedNodes;
    }

    @Override
    public void startDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        try {
            withPolicy("start discovery", allProviders, provider -> {
                    try {
                        provider.startDiscovery(cluster, node);

                        // Add to live provider only if started successfully.
                        liveProviders.add(provider);
                    } catch (HekateException | RuntimeException | Error e) {
                        // Cleanup this provider if it failed to start.
                        failSafeStop(provider, cluster, node);

                        throw e;
                    }
                }
            );
        } catch (RuntimeException | Error | HekateException e) {
            // Cleanup partially started providers.
            stopDiscovery(cluster, node);

            throw e;
        }
    }

    @Override
    public void suspendDiscovery() throws HekateException {
        liveProviders.forEach(provider -> {
            try {
                provider.suspendDiscovery();
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn("Failed to suspend discovery [provider={}]", provider, e);
                }
            }
        });
    }

    @Override
    public void stopDiscovery(String cluster, InetSocketAddress node) throws HekateException {
        try {
            for (SeedNodeProvider provider : liveProviders) {
                failSafeStop(provider, cluster, node);
            }
        } finally {
            // Cleanup live providers.
            liveProviders.clear();
        }
    }

    @Override
    public void registerRemote(String cluster, InetSocketAddress node) throws HekateException {
        withPolicy("register a remote seed node", liveProviders, provider ->
            provider.registerRemote(cluster, node)
        );
    }

    @Override
    public void unregisterRemote(String cluster, InetSocketAddress node) throws HekateException {
        withPolicy("unregister a remote seed node", liveProviders, provider ->
            provider.unregisterRemote(cluster, node)
        );
    }

    @Override
    public Collection<? extends SeedNodeProvider> jmx() {
        return allProviders;
    }

    private void withPolicy(String taskName, List<SeedNodeProvider> providers, SeedNodeProviderTask task) throws HekateException {
        assert task != null : "Task is null.";

        if (!providers.isEmpty()) {
            int success = 0;

            for (SeedNodeProvider provider : providers) {
                try {
                    task.execute(provider);

                    success++;
                } catch (HekateException e) {
                    if (policy == SeedNodeProviderGroupPolicy.FAIL_ON_FIRST_ERROR) {
                        throw e;
                    } else {
                        if (log.isWarnEnabled()) {
                            log.warn("Failed to {}.", taskName, e);
                        }
                    }
                }
            }

            if (success == 0) {
                throw new HekateException("All seed node providers failed to " + taskName + '.');
            }
        }
    }

    private void failSafeStop(SeedNodeProvider provider, String cluster, InetSocketAddress node) {
        try {
            provider.stopDiscovery(cluster, node);
        } catch (Throwable t) {
            if (log.isWarnEnabled()) {
                log.warn("Failed to stop discovery [provider={}]", provider, t);
            }
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
