/*
 * Copyright 2017 The Hekate Project
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

package io.hekate.spring.boot;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.codec.JavaCodecFactory;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.NodePropertyProvider;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.ServiceFactory;
import io.hekate.network.address.AddressSelector;
import io.hekate.network.address.DefaultAddressSelector;
import io.hekate.network.address.DefaultAddressSelectorConfig;
import io.hekate.network.address.IpVersion;
import io.hekate.spring.bean.HekateSpringBootstrap;
import io.hekate.spring.bean.codec.CodecServiceBean;
import io.hekate.spring.boot.cluster.HekateClusterServiceConfigurer;
import io.hekate.spring.boot.coordination.HekateCoordinationServiceConfigurer;
import io.hekate.spring.boot.election.HekateElectionServiceConfigurer;
import io.hekate.spring.boot.lock.HekateLockServiceConfigurer;
import io.hekate.spring.boot.messaging.HekateMessagingServiceConfigurer;
import io.hekate.spring.boot.metrics.cluster.HekateClusterMetricsServiceConfigurer;
import io.hekate.spring.boot.metrics.local.HekateLocalMetricsServiceConfigurer;
import io.hekate.spring.boot.network.HekateNetworkServiceConfigurer;
import io.hekate.spring.boot.task.HekateTaskServiceConfigurer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.springframework.boot.actuate.autoconfigure.ConditionalOnEnabledHealthIndicator;
import org.springframework.boot.actuate.autoconfigure.EndpointAutoConfiguration;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link Hekate} instances.
 *
 * <h2>Overview</h2>
 * <p>
 * This class provides support for automatic configuration and bootstrapping of {@link Hekate} instances via <a
 * href="https://projects.spring.io/spring-boot/" target="_blank">Spring Boot</a>. In order to enable this auto-configuration the
 * 'hekate-spring-boot' dependency must be added to a Spring Boot-enabled project (see 'Module dependency' section below) and {@link
 * EnableHekate @EnableHekate} annotation must be placed on application class (see <a href="#usage_example">Usage example</a> section
 * below).
 * </p>
 *
 * <h2>Module dependency</h2>
 * <p>
 * Spring Boot integration is provided by the 'hekate-spring-boot' module and can be imported into the project dependency management system
 * as in the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#maven">Maven</a></li>
 * <li><a href="#gradle">Gradle</a></li>
 * <li><a href="#ivy">Ivy</a></li>
 * </ul>
 * <div id="maven">
 * <pre>{@code
 * <dependency>
 *   <groupId>io.hekate</groupId>
 *   <artifactId>hekate-spring-boot</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-spring-boot', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-spring-boot" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <a name="usage_example"></a>
 * <h2>Usage example</h2>
 * <p>
 * The code example below shows how {@link Hekate} auto-configuration can be used in a Spring Boot-enabled applications.
 * </p>
 *
 * <p>
 * First lets define some component that depends on {@link Hekate} instance.
 * ${source: MyComponent.java#example}
 * ...and a Spring Boot application (note the {@link EnableHekate @EnableHekate} annotation)...
 * ${source: MyApplication.java#example}
 * Note that {@link Hekate} instance doesn't require any manual construction.
 * </p>
 *
 * <h2>Registering and configuring services</h2>
 * <p>
 * This class automatically registers all application-provided {@link Bean Bean}s of {@link ServiceFactory} type into the auto-configured
 * {@link Hekate} instance.
 * </p>
 *
 * <p>
 * Additionally each service has its own auto-configuration class that simplifies configuration and registration of service components.
 * Please see the documentation of the following classes:
 * </p>
 * <ul>
 * <li>{@link HekateClusterServiceConfigurer}</li>
 * <li>{@link HekateMessagingServiceConfigurer}</li>
 * <li>{@link HekateNetworkServiceConfigurer}</li>
 * <li>{@link HekateLocalMetricsServiceConfigurer}</li>
 * <li>{@link HekateClusterMetricsServiceConfigurer}</li>
 * <li>{@link HekateLockServiceConfigurer}</li>
 * <li>{@link HekateCoordinationServiceConfigurer}</li>
 * <li>{@link HekateElectionServiceConfigurer}</li>
 * <li>{@link HekateTaskServiceConfigurer}</li>
 * </ul>
 *
 * <h2>Configuration options</h2>
 * <p>
 * The following application properties can be used to configure the constructed {@link Hekate} instance:
 * </p>
 * <ul>
 * <li>{@link HekateBootstrap#setNodeName(String) 'hekate.node-name'}</li>
 * <li>{@link HekateBootstrap#setClusterName(String) 'hekate.cluster-name'}</li>
 * <li>{@link HekateBootstrap#setNodeRoles(Set) 'hekate.node-roles'}</li>
 * <li>{@link HekateBootstrap#setNodeProperties(Map) 'hekate.node-properties'}</li>
 * </ul>
 *
 * <p>
 * If application doesn't provide a {@link Bean} of {@link AddressSelector} type then {@link DefaultAddressSelector} will be registered
 * with the following configuration options:
 * </p>
 * <ul>
 * <li>{@link DefaultAddressSelectorConfig#setIpVersion(IpVersion) 'hekate.address.ip-version'}</li>
 * <li>{@link DefaultAddressSelectorConfig#setInterfaceMatch(String) 'hekate.address.interface-match'}</li>
 * <li>{@link DefaultAddressSelectorConfig#setInterfaceNotMatch(String) 'hekate.address.interface-not-match'}</li>
 * <li>{@link DefaultAddressSelectorConfig#setIpMatch(String) 'hekate.address.ip-match'}</li>
 * <li>{@link DefaultAddressSelectorConfig#setIpNotMatch(String) 'hekate.address.ip-not-match'}</li>
 * <li>{@link DefaultAddressSelectorConfig#setExcludeLoopback(boolean) 'hekate.address.exclude-loopback'}</li>
 * </ul>
 *
 * @see HekateHealthIndicator
 */
@Configuration
@ConditionalOnMissingBean(Hekate.class)
public class HekateConfigurer {
    @Configuration
    @AutoConfigureBefore(EndpointAutoConfiguration.class)
    @ConditionalOnClass(HealthIndicator.class)
    @ConditionalOnEnabledHealthIndicator("hekate")
    static class HekateHealthIndicatorConfigurer {
        @Bean
        public HekateHealthIndicator hekateHealthIndicator(Hekate node) {
            return new HekateHealthIndicator(node);
        }
    }

    private final List<ServiceFactory<?>> serviceFactories;

    private final List<Plugin> plugins;

    private final List<NodePropertyProvider> nodePropertyProviders;

    /**
     * Constructs new instance with autowired dependencies.
     *
     * @param nodePropertyProviders All {@link NodePropertyProvider}s found in the application context.
     * @param serviceFactories All {@link ServiceFactory}s found in the application context.
     * @param plugins All {@link Plugin}s found in the application context.
     */
    public HekateConfigurer(Optional<List<NodePropertyProvider>> nodePropertyProviders, Optional<List<ServiceFactory<?>>> serviceFactories,
        Optional<List<Plugin>> plugins) {
        this.serviceFactories = serviceFactories.orElse(null);
        this.plugins = plugins.orElse(null);
        this.nodePropertyProviders = nodePropertyProviders.orElse(null);
    }

    /**
     * Conditionally constructs the default codec if application doesn't provide its own {@link Bean} of {@link CodecFactory} type.
     *
     * @return Codec factory.
     *
     * @see #hekate(CodecFactory)
     */
    @Bean
    @ConditionalOnMissingBean(CodecFactory.class)
    public CodecFactory<Object> defaultCodecFactory() {
        return new JavaCodecFactory<>();
    }

    /**
     * Exports {@link CodecService} bean.
     *
     * @return {@link CodecService} bean.
     */
    @Bean
    public CodecServiceBean codecService() {
        return new CodecServiceBean();
    }

    /**
     * Constructs the {@link Hekate} factory bean.
     *
     * @param defaultCodecFactory Default codec factory (see {@link #defaultCodecFactory()}).
     *
     * @return {@link Hekate} factory bean.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate")
    public HekateSpringBootstrap hekate(CodecFactory<Object> defaultCodecFactory) {
        HekateSpringBootstrap factory = new HekateSpringBootstrap();

        factory.setDefaultCodec(defaultCodecFactory);
        factory.setServices(serviceFactories);
        factory.setPlugins(plugins);
        factory.setNodePropertyProviders(nodePropertyProviders);

        return factory;
    }
}
