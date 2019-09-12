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

package io.hekate.spring.boot.lock;

import io.hekate.core.Hekate;
import io.hekate.lock.DistributedLock;
import io.hekate.lock.LockRegion;
import io.hekate.lock.LockRegionConfig;
import io.hekate.lock.LockService;
import io.hekate.lock.LockServiceFactory;
import io.hekate.spring.bean.lock.LockBean;
import io.hekate.spring.bean.lock.LockRegionBean;
import io.hekate.spring.bean.lock.LockServiceBean;
import io.hekate.spring.boot.ConditionalOnHekateEnabled;
import io.hekate.spring.boot.HekateConfigurer;
import io.hekate.spring.boot.internal.AnnotationInjectorBase;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.BeanMetadataAttribute;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.AutowireCandidateQualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ResolvableType;
import org.springframework.stereotype.Component;

/**
 * <span class="startHere">&laquo; start here</span>Auto-configuration for {@link LockService}.
 *
 * <h2>Overview</h2>
 * <p>
 * This auto-configuration constructs a {@link Bean} of {@link LockServiceFactory} type and automatically {@link
 * LockServiceFactory#setRegions(List) registers} all {@link Bean}s of {@link LockRegionConfig} type.
 * </p>
 *
 * <p>
 * <b>Note: </b> this auto-configuration is available only if application doesn't provide its own {@link Bean} of {@link
 * LockServiceFactory} type and if there is at least one {@link Bean} of {@link LockRegionConfig} type within the application context.
 * </p>
 *
 * <h2>Configuration Properties</h2>
 * <p>
 * It is possible to configure {@link LockServiceFactory} via application properties prefixed with {@code 'hekate.locks'}.
 * For example:
 * </p>
 * <ul>
 * <li>{@link LockServiceFactory#setNioThreads(int) 'hekate.locks.nio-threads'}</li>
 * <li>{@link LockServiceFactory#setWorkerThreads(int) 'hekate.locks.worker-threads'}</li>
 * <li>{@link LockServiceFactory#setRetryInterval(long) 'hekate.locks.retry-interval'}</li>
 * </ul>
 *
 * <h2>Locks Injections</h2>
 * <p>
 * This auto-configuration provides support for injecting beans of {@link LockRegion} and {@link DistributedLock} type into other beans with
 * the help of {@link InjectLockRegion} and {@link InjectLock} annotations.
 * </p>
 *
 * <p>
 * Please see the documentation of the following annotations for more details:
 * </p>
 * <ul>
 * <li>{@link InjectLockRegion} - for injection of {@link LockRegion}s</li>
 * <li>{@link InjectLock} - for injection of {@link DistributedLock}s</li>
 * </ul>
 *
 * @see LockService
 * @see HekateConfigurer
 */
@Configuration
@ConditionalOnHekateEnabled
@AutoConfigureBefore(HekateConfigurer.class)
@ConditionalOnBean(LockRegionConfig.class)
@ConditionalOnMissingBean(LockServiceFactory.class)
public class HekateLockServiceConfigurer {
    @Component
    static class LockRegionInjector extends AnnotationInjectorBase<InjectLockRegion> {
        public LockRegionInjector() {
            super(InjectLockRegion.class, LockRegion.class);
        }

        @Override
        protected void registerBeans(InjectLockRegion annotation, ResolvableType targetType, BeanDefinitionRegistry registry) {
            String name = LockRegionBean.class.getName() + "-" + annotation.value();

            if (!registry.containsBeanDefinition(name)) {
                AbstractBeanDefinition def = BeanDefinitionBuilder.rootBeanDefinition(LockRegionBean.class)
                    .setLazyInit(true)
                    .addPropertyValue("region", annotation.value())
                    .getBeanDefinition();

                def.addQualifier(new AutowireCandidateQualifier(annotation.annotationType(), annotation.value()));

                registry.registerBeanDefinition(name, def);
            }
        }
    }

    @Component
    static class LockInjector extends AnnotationInjectorBase<InjectLock> {
        public LockInjector() {
            super(InjectLock.class, DistributedLock.class);
        }

        @Override
        protected void registerBeans(InjectLock annotation, ResolvableType targetType, BeanDefinitionRegistry registry) {
            String name = LockBean.class.getName() + "-" + annotation.name() + "--" + annotation.name();

            if (!registry.containsBeanDefinition(name)) {
                AbstractBeanDefinition def = BeanDefinitionBuilder.rootBeanDefinition(LockBean.class)
                    .setLazyInit(true)
                    .addPropertyValue("region", annotation.region())
                    .addPropertyValue("name", annotation.name())
                    .getBeanDefinition();

                AutowireCandidateQualifier qualifier = new AutowireCandidateQualifier(annotation.annotationType());
                qualifier.addMetadataAttribute(new BeanMetadataAttribute("region", annotation.region()));
                qualifier.addMetadataAttribute(new BeanMetadataAttribute("name", annotation.name()));

                def.addQualifier(qualifier);

                registry.registerBeanDefinition(name, def);
            }
        }
    }

    private final List<LockRegionConfig> regions;

    /**
     * Constructs new instance.
     *
     * @param regions {@link LockRegionConfig}s that were found in the application context.
     */
    public HekateLockServiceConfigurer(Optional<List<LockRegionConfig>> regions) {
        this.regions = regions.orElse(null);
    }

    /**
     * Constructs the {@link LockServiceFactory}.
     *
     * @return Service factory.
     */
    @Bean
    @ConfigurationProperties(prefix = "hekate.locks")
    public LockServiceFactory lockServiceFactory() {
        LockServiceFactory factory = new LockServiceFactory();

        factory.setRegions(regions);

        return factory;
    }

    /**
     * Returns the factory bean that makes it possible to inject {@link LockService} directly into other beans instead of accessing it via
     * {@link Hekate#locks()} method.
     *
     * @return Service bean.
     */
    @Lazy
    @Bean
    public LockServiceBean lockService() {
        return new LockServiceBean();
    }
}
