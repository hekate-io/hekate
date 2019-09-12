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

package io.hekate.spring.boot.rpc;

import io.hekate.core.Hekate;
import io.hekate.messaging.MessagingBackPressureConfig;
import io.hekate.messaging.MessagingOverflowPolicy;
import io.hekate.rpc.RpcClientConfig;
import io.hekate.rpc.RpcServerConfig;
import io.hekate.rpc.RpcService;
import io.hekate.rpc.RpcServiceFactory;
import io.hekate.spring.bean.rpc.RpcClientBean;
import io.hekate.spring.bean.rpc.RpcServiceBean;
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
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link RpcService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link RpcServiceFactory} type and automatically registers all {@link Bean}s of
 * {@link RpcClientConfig} and {@link RpcServerConfig} types.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link RpcServiceFactory}
 * type.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * It is possible to configure {@link RpcServiceFactory} via application properties prefixed with {@code 'hekate.rpc'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link RpcServiceFactory#setNioThreads(int) 'hekate.rpc.nio-threads'}</li>
 * <li>{@link RpcServiceFactory#setWorkerThreads(int) 'hekate.rpc.worker-threads'}</li>
 * <li>{@link RpcServiceFactory#setIdleSocketTimeout(long) 'hekate.rpc.idle-socket-timeout'}</li>
 * <li>{@link MessagingBackPressureConfig#setInLowWatermark(int) 'hekate.rpc.back-bressure.in-low-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setInHighWatermark(int) 'hekate.rpc.back-bressure.in-high-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setOutLowWatermark(int) 'hekate.rpc.back-bressure.out-low-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setOutHighWatermark(int) 'hekate.rpc.back-bressure.out-high-watermark'}</li>
 * <li>{@link MessagingBackPressureConfig#setOutOverflowPolicy(MessagingOverflowPolicy) 'hekate.rpc.back-bressure.out-overflow-policy'}</li>
 * </ul>
 *
 * <h2>Injection of RPC Client Proxies</h2>
 * <p>
 * This auto-configuration provides support for injection of RPC client proxies into other beans with the help of {@link InjectRpcClient}
 * annotation. Please see its documentation for more details.
 * </p>
 *
 * @see RpcService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnMissingBean(RpcServiceFactory.class)
public class HekateRpcServiceConfigurer {
    @Component
    static class RpcClientInjector extends AnnotationInjectorBase<InjectRpcClient> {
        public RpcClientInjector() {
            super(InjectRpcClient.class, Object.class);
        }

        @Override
        protected void registerBeans(InjectRpcClient annotation, ResolvableType targetType, BeanDefinitionRegistry registry) {
            String tag = annotation.tag().isEmpty() ? "" : '#' + annotation.tag();

            String name = RpcClientBean.class.getName() + tag + "-" + targetType.toString();

            if (!registry.containsBeanDefinition(name)) {
                BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(RpcClientBean.class)
                    .setLazyInit(true)
                    .addPropertyValue("rpcInterface", targetType.getType().getTypeName());

                if (!annotation.tag().isEmpty()) {
                    builder.addPropertyValue("tag", annotation.tag());
                }

                AbstractBeanDefinition def = builder.getBeanDefinition();

                AutowireCandidateQualifier qualifier = new AutowireCandidateQualifier(annotation.annotationType());

                if (!annotation.tag().isEmpty()) {
                    qualifier.setAttribute("tag", annotation.tag());
                }

                def.addQualifier(qualifier);

                registry.registerBeanDefinition(name, def);
            }
        }
    }

    private final List<RpcClientConfig> clients;

    private final List<RpcServerConfig> servers;

    /**
     * Constructs new instance.
     *
     * @param clients {@link RpcClientConfig}s that were found in the application context.
     * @param servers {@link RpcServerConfig}s that were found in the application context.
     */
    public HekateRpcServiceConfigurer(Optional<List<RpcClientConfig>> clients, Optional<List<RpcServerConfig>> servers) {
        this.clients = clients.orElse(null);
        this.servers = servers.orElse(null);
    }

    /**
     * Constructs the {@link RpcServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.rpc")
    public RpcServiceFactory rpcServiceFactory() {
        RpcServiceFactory factory = new RpcServiceFactory();

        factory.setClients(clients);
        factory.setServers(servers);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link RpcService} directly into other beans instead of accessing it via
     * {@link Hekate#rpc()} method.
     *
     * @return Service bean.
     */
    @Lazy
    @Bean
    public RpcServiceBean rpcService() {
        return new RpcServiceBean();
    }
}
