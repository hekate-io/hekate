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

package io.hekate.spring.boot.cluster;

import io.hekate.cluster.ClusterAcceptor;
import io.hekate.cluster.ClusterService;
import io.hekate.cluster.ClusterServiceFactory;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.health.DefaultFailureDetector;
import io.hekate.cluster.health.DefaultFailureDetectorConfig;
import io.hekate.cluster.health.FailureDetector;
import io.hekate.cluster.seed.SeedNodeProvider;
import io.hekate.cluster.seed.SeedNodeProviderGroup;
import io.hekate.cluster.seed.SeedNodeProviderGroupConfig;
import io.hekate.cluster.seed.SeedNodeProviderGroupPolicy;
import io.hekate.cluster.split.SplitBrainAction;
import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.Hekate;
import io.hekate.spring.bean.cluster.ClusterServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import java.util.List;
import java.util.Optional;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link ClusterService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link ClusterServiceFactory} type and auto-wires it with the following beans from
 * the application context:
 * </p>
 * <ul>
 * <li>{@link SeedNodeProvider}</li>
 * <li>{@link ClusterEventListener}</li>
 * <li>{@link ClusterAcceptor}</li>
 * <li>{@link SplitBrainDetector}</li>
 * </ul>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * ClusterServiceFactory} type.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * It is possible to configure {@link ClusterServiceFactory} via application properties prefixed with {@code 'hekate.cluster'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link ClusterServiceFactory#setGossipInterval(long) 'hekate.cluster.gossip-interval'}</li>
 * <li>{@link ClusterServiceFactory#setSpeedUpGossipSize(int) 'hekate.cluster.speed-up-gossip-size'}</li>
 * <li>{@link ClusterServiceFactory#setSplitBrainAction(SplitBrainAction) 'hekate.cluster.split-brain-action'}</li>
 * </ul>
 *
 * <p>
 * Additionally, if application doesn't provide its own {@link Bean} of {@link FailureDetector} type then {@link DefaultFailureDetector}
 * will be registered and can be configured via the following properties:
 * </p>
 * <ul>
 * <li>{@link DefaultFailureDetectorConfig#setHeartbeatInterval(long) 'hekate.cluster.health.heartbeat-interval'}</li>
 * <li>{@link DefaultFailureDetectorConfig#setHeartbeatLossThreshold(int) 'hekate.cluster.health.heartbeat-loss-threshold'}</li>
 * <li>{@link DefaultFailureDetectorConfig#setFailureDetectionQuorum(int) 'hekate.cluster.health.failure-detection-quorum'}</li>
 * <li>{@link DefaultFailureDetectorConfig#setFailFast(boolean)} 'hekate.cluster.health.fail-fast'}</li>
 * </ul>
 *
 * <h2>Seed Node Providers</h2>
 * <p>
 * This auto-configuration registers all beans of the {@link SeedNodeProvider} type that can be found in the application context.
 * All such providers will be registered as a single {@link SeedNodeProviderGroup}. Rrror handling policy of that group can be specified
 * via the {@link SeedNodeProviderGroupConfig#setPolicy(SeedNodeProviderGroupPolicy) 'hekate.cluster.seed.policy'} property.
 * </p>
 *
 * <p>
 * For auto-configuration of {@link SeedNodeProvider}s please see the documentation of the following classes:
 * </p>
 * <ul>
 * <li>{@link HekateMulticastSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateFsSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateJdbcSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateZooKeeperSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateCloudStoreSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateCloudSeedNodeProviderConfigurer}</li>
 * <li>{@link HekateStaticSeedNodeProviderConfigurer}</li>
 * </ul>
 *
 * @see ClusterService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(ClusterServiceFactory.class)
public class HekateClusterServiceConfigurer {
    private final Optional<List<SeedNodeProvider>> seedNodeProviders;

    private final Optional<List<ClusterEventListener>> listeners;

    private final Optional<List<ClusterAcceptor>> acceptors;

    private final Optional<SplitBrainDetector> splitBrainDetector;

    /**
     * Constructs new instance.
     *
     * @param seedNodeProviders {@link SeedNodeProvider}s that were found in the application context.
     * @param listeners All {@link ClusterEventListener}s that were found in the application context.
     * @param acceptors All {@link ClusterAcceptor}s that were found in the application context.
     * @param splitBrainDetector {@link SplitBrainDetector} that was found in the application context.
     */
    public HekateClusterServiceConfigurer(
        Optional<List<SeedNodeProvider>> seedNodeProviders,
        Optional<List<ClusterEventListener>> listeners,
        Optional<List<ClusterAcceptor>> acceptors,
        Optional<SplitBrainDetector> splitBrainDetector
    ) {
        this.seedNodeProviders = seedNodeProviders;
        this.listeners = listeners;
        this.acceptors = acceptors;
        this.splitBrainDetector = splitBrainDetector;
    }

    /**
     * Conditionally constructs the configuration for a default failure detector if application doesn't provide its own {@link Bean} of
     * {@link FailureDetector} type.
     *
     * @return Configuration for {@link DefaultFailureDetector}.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.cluster.health")
    @ConditionalOnMissingBean({FailureDetector.class, DefaultFailureDetectorConfig.class})
    public DefaultFailureDetectorConfig defaultFailureDetectorConfig() {
        return new DefaultFailureDetectorConfig();
    }

    /**
     * Conditionally constructs the default failure detector if application doesn't provide its own {@link Bean} of
     * {@link FailureDetector} type.
     *
     * @param cfg Configuration (see {@link #defaultFailureDetectorConfig()}).
     *
     * @return Failure detector.
     */
    @Bean
    @ConditionalOnMissingBean(FailureDetector.class)
    public DefaultFailureDetector defaultFailureDetector(DefaultFailureDetectorConfig cfg) {
        return new DefaultFailureDetector(cfg);
    }

    /**
     * Constructs a group configuration of all {@link SeedNodeProvider}s that were found in the application context.
     *
     * @return Configuration of seed node provider group
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.cluster.seed")
    public SeedNodeProviderGroupConfig seedNodeProviderGroupConfig() {
        return new SeedNodeProviderGroupConfig().withProviders(seedNodeProviders.orElse(null));
    }

    /**
     * Constructs the {@link ClusterServiceFactory}.
     *
     * @param failureDetector Failure detector.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.cluster")
    public ClusterServiceFactory clusterServiceFactory(Optional<FailureDetector> failureDetector) {
        ClusterServiceFactory factory = new ClusterServiceFactory();

        failureDetector.ifPresent(factory::setFailureDetector);

        listeners.ifPresent(factory::setClusterListeners);
        splitBrainDetector.ifPresent(factory::setSplitBrainDetector);
        acceptors.ifPresent(factory::setAcceptors);

        SeedNodeProviderGroupConfig seedGroupCfg = seedNodeProviderGroupConfig();

        if (seedGroupCfg.hasProviders()) {
            factory.setSeedNodeProvider(new SeedNodeProviderGroup(seedGroupCfg));
        }

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link ClusterService} directly into other beans instead of accessing it
     * via {@link Hekate#cluster()} method.
     *
     * @return Service bean.
     */
    @Lazy
    @Bean
    public ClusterServiceBean clusterService() {
        return new ClusterServiceBean();
    }
}
