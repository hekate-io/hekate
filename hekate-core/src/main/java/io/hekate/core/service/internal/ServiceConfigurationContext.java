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

package io.hekate.core.service.internal;

import io.hekate.core.ServiceInfo;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.Service;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toSet;

class ServiceConfigurationContext implements ConfigurationContext {
    private static class ServiceRef {
        private final Set<String> serviceTypes;

        private final ServiceRef parent;

        public ServiceRef(Set<String> serviceTypes, ServiceRef parent) {
            this.serviceTypes = serviceTypes;
            this.parent = parent;
        }

        public Set<String> serviceTypes() {
            return serviceTypes;
        }

        public ServiceRef parent() {
            return parent;
        }
    }

    private final Map<String, Map<String, String>> props = new HashMap<>();

    @ToStringIgnore
    private final ServiceManager manager;

    @ToStringIgnore
    private ServiceRef current;

    public ServiceConfigurationContext(ServiceManager manager) {
        this.manager = manager;
    }

    @Override
    public void setServiceProperty(String name, String value) {
        ArgAssert.notNull(name, "Property name");

        if (value != null && !value.isEmpty()) {
            checkState();

            current.serviceTypes().forEach(type ->
                props.get(type).put(name, value)
            );
        }
    }

    @Override
    public <T> Collection<T> findComponents(Class<T> type) {
        List<T> result = new ArrayList<>();

        List<ServiceHandler> handlers = manager.getHandlers();

        // Use index-based iteration since new handlers can be added dynamically during services configuration.
        for (int i = 0; i < handlers.size(); i++) {
            ServiceHandler handler = handlers.get(i);

            Service service = handler.service();

            if (type.isAssignableFrom(service.getClass())) {
                handler.configure(this);

                result.add(type.cast(service));
            }
        }

        return result;
    }

    public void prepare(Object service) {
        Set<String> typeNames = serviceInterfaces(service).stream()
            .map(Class::getCanonicalName)
            .collect(toSet());

        typeNames.forEach(type ->
            props.put(type, new HashMap<>())
        );

        current = new ServiceRef(typeNames, current);
    }

    public void close() {
        current = current.parent();
    }

    public Map<String, ServiceInfo> servicesInfo() {
        Map<String, ServiceInfo> info = new HashMap<>();

        props.forEach((type, props) -> {
            Map<String, String> propsCopy = new HashMap<>();

            props.forEach(propsCopy::put);

            info.put(type, new DefaultServiceInfo(type, unmodifiableMap(propsCopy)));
        });

        return info;
    }

    private void checkState() {
        if (current == null) {
            throw new IllegalStateException("Configuration context can be accessed during service configuration.");
        }
    }

    private static Set<Class<? extends Service>> serviceInterfaces(Object service) {
        Set<Class<? extends Service>> faces = new HashSet<>();

        for (Class<?> type : service.getClass().getInterfaces()) {
            if (!Service.class.equals(type) && Service.class.isAssignableFrom(type)) {
                faces.add(type.asSubclass(Service.class));
            }
        }

        return faces;
    }

    @Override
    public String toString() {
        return ToString.format(ConfigurationContext.class, this);
    }
}
