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

package io.hekate.spring.boot.network;

import io.hekate.core.Hekate;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkConnectorConfig;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.NetworkSslConfig;
import io.hekate.spring.bean.network.NetworkConnectorBean;
import io.hekate.spring.bean.network.NetworkServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.spring.boot.internal.AnnotationInjectorBase;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link NetworkService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link NetworkServiceFactory} type and automatically {@link
 * NetworkServiceFactory#setConnectors(List) registers} all {@link Bean}s of {@link NetworkConnectorConfig} type.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * NetworkServiceFactory} type.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * It is possible to configure {@link NetworkServiceFactory} via application properties prefixed with {@code 'hekate.network'}:
 * </p>
 * <ul>
 * <li>{@link NetworkServiceFactory#setHost(String) 'hekate.network.host'}</li>
 * <li>{@link NetworkServiceFactory#setPort(int) 'hekate.network.port'}</li>
 * <li>{@link NetworkServiceFactory#setPortRange(int) 'hekate.network.port-range'}</li>
 * <li>{@link NetworkServiceFactory#setConnectTimeout(int) 'hekate.network.connect-timeout'}</li>
 * <li>{@link NetworkServiceFactory#setAcceptRetryInterval(long) 'hekate.network.server-failover-interval'}</li>
 * <li>{@link NetworkServiceFactory#setHeartbeatInterval(int) 'hekate.network.heartbeat-interval'}</li>
 * <li>{@link NetworkServiceFactory#setHeartbeatLossThreshold(int) 'hekate.network.heartbeat-loss-threshold'}</li>
 * <li>{@link NetworkServiceFactory#setNioThreads(int) 'hekate.network.nio-thread-pool-size'}</li>
 * <li>{@link NetworkServiceFactory#setTcpNoDelay(boolean) 'hekate.network.tcp-no-delay'}</li>
 * <li>{@link NetworkServiceFactory#setTcpReceiveBufferSize(Integer) 'hekate.network.tcp-receive-buffer-size'}</li>
 * <li>{@link NetworkServiceFactory#setTcpSendBufferSize(Integer) 'hekate.network.tcp-send-buffer-size'}</li>
 * <li>{@link NetworkServiceFactory#setTcpReuseAddress(Boolean) 'hekate.network.tcp-reuse-address'}</li>
 * <li>{@link NetworkServiceFactory#setTcpBacklog(Integer) 'hekate.network.tcp-backlog'}</li>
 * </ul>
 *
 * <h2>SSL/TLS Configuration</h2>
 * <p>
 * SSL/TLS encryption can be enabled by setting the {@code 'hekate.network.ssl.enable'} configuration property to {@code true} and using the
 * following properties:
 * </p>
 * <ul>
 * <li>{@link NetworkSslConfig#setProvider(NetworkSslConfig.Provider) 'hekate.network.ssl.provider'}</li>
 * <li>{@link NetworkSslConfig#setKeyStorePath(String) 'hekate.network.ssl.key-store-path'}</li>
 * <li>{@link NetworkSslConfig#setKeyStorePassword(String) 'hekate.network.ssl.key-store-password'}</li>
 * <li>{@link NetworkSslConfig#setKeyStoreType(String) 'hekate.network.ssl.key-store-type'}</li>
 * <li>{@link NetworkSslConfig#setKeyStoreAlgorithm(String) 'hekate.network.ssl.key-store-algorithm'}</li>
 * <li>{@link NetworkSslConfig#setTrustStorePath(String) 'hekate.network.ssl.trust-store-path'}</li>
 * <li>{@link NetworkSslConfig#setTrustStorePassword(String) 'hekate.network.ssl.trust-store-password'}</li>
 * <li>{@link NetworkSslConfig#setTrustStoreType(String) 'hekate.network.ssl.trust-store-type'}</li>
 * <li>{@link NetworkSslConfig#setTrustStoreAlgorithm(String) 'hekate.network.ssl.trust-store-algorithm'}</li>
 * </ul>
 *
 * <h2>Connectors Injection</h2>
 * <p>
 * This auto-configuration provides support for injecting beans of {@link NetworkConnector} type into other beans with the help of {@link
 * InjectConnector} annotation. Please see its documentation for more details.
 * </p>
 *
 * @see NetworkService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(NetworkServiceFactory.class)
public class HekateNetworkServiceConfigurer {
    @Component
    static class NetworkConnectorInjector extends AnnotationInjectorBase<InjectConnector> {
        public NetworkConnectorInjector() {
            super(InjectConnector.class, NetworkConnector.class);
        }

        @Override
        protected void registerBeans(InjectConnector annotation, ResolvableType targetType, BeanDefinitionRegistry registry) {
            String name = NetworkConnectorBean.class.getName() + "-" + annotation.value();

            if (!registry.containsBeanDefinition(name)) {
                AbstractBeanDefinition def = BeanDefinitionBuilder.rootBeanDefinition(NetworkConnectorBean.class)
                    .setLazyInit(true)
                    .addPropertyValue("protocol", annotation.value())
                    .getBeanDefinition();

                def.addQualifier(new AutowireCandidateQualifier(annotation.annotationType(), annotation.value()));

                registry.registerBeanDefinition(name, def);
            }
        }
    }

    private final List<NetworkConnectorConfig<?>> connectors;

    /**
     * Constructs new instance.
     *
     * @param connectors {@link NetworkConnectorConfig}s that were found in the application context.
     */
    public HekateNetworkServiceConfigurer(Optional<List<NetworkConnectorConfig<?>>> connectors) {
        this.connectors = connectors.orElse(null);
    }

    /**
     * Constructs a {@link NetworkSslConfig}.
     *
     * @return SSL configuration.
     *
     * @see NetworkServiceFactory#setSsl(NetworkSslConfig)
     */
    @Bean
    @ConditionalOnMissingBean(NetworkSslConfig.class)
    @ConfigurationProperties(prefix = "hekate.network.ssl")
    @ConditionalOnProperty(value = "hekate.network.ssl.enable", havingValue = "true")
    public NetworkSslConfig networkSslConfig() {
        return new NetworkSslConfig();
    }

    /**
     * Constructs the {@link NetworkServiceFactory}.
     *
     * @param ssl Optional SSL configuration.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.network")
    public NetworkServiceFactory networkServiceFactory(Optional<NetworkSslConfig> ssl) {
        NetworkServiceFactory factory = new NetworkServiceFactory();

        factory.setConnectors(connectors);

        ssl.ifPresent(factory::withSsl);

        return factory;
    }

    /**
     * Returns a factory bean that makes it possible to inject {@link NetworkService} directly into other beans instead of accessing it
     * via {@link Hekate#network()} method.
     *
     * @return Service bean.
     */
    @Bean
    public NetworkServiceBean networkService() {
        return new NetworkServiceBean();
    }
}
