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

package io.hekate.spring.bean;

import io.hekate.core.inject.HekateInject;
import io.hekate.core.inject.InjectionService;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

class SpringInjectionService implements InjectionService, ConfigurableService {
    private final ApplicationContext parentCtx;

    private AutowireCapableBeanFactory autowire;

    public SpringInjectionService(ApplicationContext parentCtx) {
        assert parentCtx != null : "Application context is null.";

        this.parentCtx = parentCtx;
    }

    public static ServiceFactory<InjectionService> factory(ApplicationContext ctx) {
        assert ctx != null : "Application context is null.";

        return new ServiceFactory<InjectionService>() {
            @Override
            public InjectionService createService() {
                return new SpringInjectionService(ctx);
            }

            @Override
            public String toString() {
                return SpringInjectionService.class.getSimpleName() + "Factory";
            }
        };
    }

    @Override
    public void inject(Object obj) {
        Class<?> type = obj.getClass();

        boolean inject = type.isAnnotationPresent(HekateInject.class);

        if (inject) {
            autowire.autowireBeanProperties(obj, AutowireCapableBeanFactory.AUTOWIRE_NO, true);
        }
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        // Application context for autowiring.
        AnnotationConfigApplicationContext autowireCtx = new AnnotationConfigApplicationContext() {
            @Override
            public String toString() {
                return SpringInjectionService.class.getSimpleName() + "Context";
            }
        };

        // Expose services for autowiring.
        ConfigurableListableBeanFactory factory = autowireCtx.getBeanFactory();

        uniqueServices(ctx).forEach(service -> {
            factory.registerResolvableDependency(service.getClass(), service);

            for (Class<?> type : service.getClass().getInterfaces()) {
                factory.registerResolvableDependency(type, service);
            }
        });

        autowireCtx.refresh();

        autowireCtx.setParent(parentCtx);

        autowire = autowireCtx.getAutowireCapableBeanFactory();
    }

    private Set<Service> uniqueServices(ConfigurationContext ctx) {
        Collection<Service> services = ctx.findComponents(Service.class);

        Map<Service, Void> uniqueServices = new IdentityHashMap<>(services.size());

        services.forEach(service -> uniqueServices.put(service, null));

        return uniqueServices.keySet();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
