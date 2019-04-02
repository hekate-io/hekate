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

package io.hekate.spring.boot;

import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecService;
import io.hekate.core.Hekate;
import io.hekate.core.Hekate.LifecycleListener;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateFutureException;
import io.hekate.core.PropertyProvider;
import io.hekate.core.plugin.Plugin;
import io.hekate.core.service.ServiceFactory;
import io.hekate.spring.bean.HekateSpringBootstrap;
import io.hekate.spring.bean.codec.CodecServiceBean;
import io.hekate.spring.boot.cluster.HekateClusterServiceConfigurer;
import io.hekate.spring.boot.coordination.HekateCoordinationServiceConfigurer;
import io.hekate.spring.boot.election.HekateElectionServiceConfigurer;
import io.hekate.spring.boot.lock.HekateLockServiceConfigurer;
import io.hekate.spring.boot.messaging.HekateMessagingServiceConfigurer;
import io.hekate.spring.boot.network.HekateNetworkServiceConfigurer;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContextException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.stereotype.Component;

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
 * <h2>Module Dependency</h2>
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
 * <h2>Usage Example</h2>
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
 * <h2>Registering and Configuring Services</h2>
 * <p>
 * This class automatically registers all application-provided {@link Bean Bean}s of {@link ServiceFactory} type into the auto-configured
 * {@link Hekate} instance.
 * </p>
 *
 * <p>
 * Additionally each service has its own auto-configuration class that simplifies configuration and registration of services and
 * components.
 * Please see the documentation of the following classes:
 * </p>
 * <ul>
 * <li>{@link HekateClusterServiceConfigurer}</li>
 * <li>{@link HekateMessagingServiceConfigurer}</li>
 * <li>{@link HekateNetworkServiceConfigurer}</li>
 * <li>{@link HekateLockServiceConfigurer}</li>
 * <li>{@link HekateCoordinationServiceConfigurer}</li>
 * <li>{@link HekateElectionServiceConfigurer}</li>
 * </ul>
 *
 * <h2>Configuration Options</h2>
 * <p>
 * The following application properties can be used to configure the constructed {@link Hekate} instance:
 * </p>
 * <ul>
 * <li>{@link HekateBootstrap#setNodeName(String) 'hekate.node-name'}</li>
 * <li>{@link HekateBootstrap#setClusterName(String) 'hekate.cluster-name'}</li>
 * <li>{@link HekateBootstrap#setRoles(List) 'hekate.roles'}</li>
 * <li>{@link HekateBootstrap#setProperties(Map) 'hekate.properties'}</li>
 * </ul>
 *
 * <h2>Deferred Cluster Joining</h2>
 * <p>
 * It is possible to control the timing of when {@link Hekate} node will start joining the cluster by specifying the following properties:
 * </p>
 * <ul>
 * <li>{@link HekateSpringBootstrap#setDeferredJoin(boolean) 'hekate.deferred-join'} - if set to {@code false} (default value) then joining
 * will happen synchronously during the Spring Application context initialization</li>
 * <li>{@link HekateSpringBootstrap#setDeferredJoin(boolean) 'hekate.deferred-join'} - if set to {@code true} then joining will be deferred
 * until the Spring Application context gets fully initialized (signalled by {@link ApplicationReadyEvent}).</li>
 * </ul>
 *
 * <p>
 * Furthermore it is also possible to completely disable joining to the cluster by setting the {@code 'hekate.deferred-join-condition'}
 * property value to {@code 'manual'}. In such case it is up to the application logic to decide on when to call the {@link Hekate#join()}
 * method in order to start joining the cluster. Alternative value of this property is {@code 'app-ready'} which will fall back to the
 * default behavior.
 * </p>
 */
@Configuration
@ConditionalOnMissingBean(Hekate.class)
@ConditionalOnProperty(name = "hekate.enable", havingValue = "true", matchIfMissing = true)
public class HekateConfigurer {
    /**
     * Handler for {@link HekateSpringBootstrap#setDeferredJoin(boolean) deferred join}.
     *
     * <p>
     * Performs {@link Hekate#join()} when {@link ApplicationReadyEvent} gets fired.
     * </p>
     */
    @Component
    @ConditionalOnProperty(value = "hekate.deferred-join-condition", havingValue = "app-ready", matchIfMissing = true)
    static class HekateDeferredJoinReadyHandler implements ApplicationListener<ApplicationReadyEvent> {
        private final Hekate hekate;

        /**
         * Constructs a new instance.
         *
         * @param hekate Node.
         */
        public HekateDeferredJoinReadyHandler(Hekate hekate) {
            this.hekate = hekate;
        }

        @Override
        public void onApplicationEvent(ApplicationReadyEvent event) {
            try {
                // Handle deferred join when application is ready.
                if (hekate.state() == Hekate.State.INITIALIZED) {
                    hekate.join();
                }
            } catch (InterruptedException e) {
                throw new ApplicationContextException("Thread got interrupted while awaiting for cluster join.", e);
            } catch (HekateFutureException e) {
                throw new ApplicationContextException("Failed to join the cluster.", e);
            }
        }
    }

    private final CodecFactory<Object> codec;

    private final List<ServiceFactory<?>> services;

    private final List<Plugin> plugins;

    private final List<PropertyProvider> propertyProviders;

    private final List<LifecycleListener> listeners;

    private final MeterRegistry metrics;

    /**
     * Constructs new instance with autowired dependencies.
     *
     * @param propertyProviders All {@link PropertyProvider}s found in the application context.
     * @param services All {@link ServiceFactory}s found in the application context.
     * @param plugins All {@link Plugin}s found in the application context.
     * @param listeners All {@link LifecycleListener}s found in the application context.
     * @param metrics Metrics registry.
     * @param codec Default codec factory.
     */
    public HekateConfigurer(
        Optional<List<PropertyProvider>> propertyProviders,
        Optional<List<ServiceFactory<?>>> services,
        Optional<List<Plugin>> plugins,
        Optional<List<LifecycleListener>> listeners,
        Optional<MeterRegistry> metrics,
        @Qualifier("default") Optional<CodecFactory<Object>> codec
    ) {
        this.services = services.orElse(null);
        this.plugins = plugins.orElse(null);
        this.propertyProviders = propertyProviders.orElse(null);
        this.listeners = listeners.orElse(null);
        this.codec = codec.orElse(null);
        this.metrics = metrics.orElse(null);
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
     * @return {@link Hekate} factory bean.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate")
    public HekateSpringBootstrap hekate() {
        HekateSpringBootstrap boot = new HekateSpringBootstrap();

        boot.setServices(services);
        boot.setPlugins(plugins);
        boot.setPropertyProviders(propertyProviders);
        boot.setLifecycleListeners(listeners);
        boot.setMetrics(metrics);

        if (codec != null) {
            boot.setDefaultCodec(codec);
        }

        return boot;
    }
}
