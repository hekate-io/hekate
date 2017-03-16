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

import io.hekate.cluster.ClusterNodeService;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.Service;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toSet;

class ServiceConfigurationContext implements ConfigurationContext {
    private static class ServiceElement {
        private final Set<String> serviceTypes;

        private final Object instance;

        private final ServiceElement parent;

        public ServiceElement(Object instance, Set<String> serviceTypes, ServiceElement parent) {
            this.serviceTypes = serviceTypes;
            this.instance = instance;
            this.parent = parent;
        }

        public Set<String> getServiceTypes() {
            return serviceTypes;
        }

        public Object getInstance() {
            return instance;
        }

        public ServiceElement getParent() {
            return parent;
        }
    }

    private final Set<String> roles;

    private final Map<String, String> properties;

    private final Map<String, Map<String, Set<String>>> serviceProps = new HashMap<>();

    @ToStringIgnore
    private final ServiceManager manager;

    @ToStringIgnore
    private ServiceElement currentService;

    public ServiceConfigurationContext(Set<String> roles, Map<String, String> properties, ServiceManager manager) {
        this.roles = new HashSet<>(roles);
        this.properties = new HashMap<>(properties);
        this.manager = manager;
    }

    @Override
    public Set<String> getNodeRoles() {
        return Collections.unmodifiableSet(new HashSet<>(roles));
    }

    @Override
    public void addNodeRole(String role) {
        ArgAssert.notNull(role, "Role");

        checkState();

        roles.add(role.trim());
    }

    @Override
    public Map<String, String> getNodeProperties() {
        return Collections.unmodifiableMap(new HashMap<>(properties));
    }

    @Override
    public void addNodeProperty(String name, String value) {
        checkState();

        if (name != null) {
            name = name.trim();
        }

        if (value != null) {
            value = value.trim();
        }

        ArgAssert.check(!properties.containsKey(name), "Property name already registered [name=" + name + ']');

        properties.put(name, value);
    }

    @Override
    public void addServiceProperty(String name, String value) {
        ArgAssert.notNull(name, "Property name");

        if (value != null && !value.isEmpty()) {
            checkState();

            currentService.getServiceTypes().forEach(type -> {
                Map<String, Set<String>> props = serviceProps.get(type);

                Set<String> values = props.computeIfAbsent(name, k -> new HashSet<>());

                values.add(value);
            });
        }
    }

    @Override
    public <T> Collection<T> findComponents(Class<T> type) {
        List<T> result = new ArrayList<>();

        List<ServiceHandler> handlers = manager.getHandlers();

        // Use index-based iteration since new handlers can be added dynamically during services configuration.
        for (int i = 0; i < handlers.size(); i++) {
            ServiceHandler handler = handlers.get(i);

            Service service = handler.getService();

            if (type.isAssignableFrom(service.getClass())) {
                handler.configure(this);

                result.add(type.cast(service));
            }
        }

        return result;
    }

    public void prepare(Object service) {
        Set<Class<? extends Service>> interfaces = getServiceInterfaces(service);

        Set<String> serviceTypes = interfaces.stream().map(Class::getCanonicalName).collect(toSet());

        currentService = new ServiceElement(service, serviceTypes, currentService);

        serviceTypes.forEach(type -> serviceProps.put(type, new HashMap<>()));
    }

    public void close() {
        currentService = currentService.getParent();
    }

    public Map<String, ClusterNodeService> getServicesInfo() {
        Map<String, ClusterNodeService> info = new HashMap<>();

        serviceProps.forEach((type, props) -> {
            Map<String, Set<String>> propsCopy = new HashMap<>();

            props.forEach((propName, propVal) -> propsCopy.put(propName, Collections.unmodifiableSet(new HashSet<>(propVal))));

            info.put(type, new DefaultClusterNodeService(type, Collections.unmodifiableMap(propsCopy)));
        });

        return info;
    }

    private void checkState() {
        if (currentService == null) {
            throw new IllegalStateException("Configuration context can be accessed during service configuration.");
        }
    }

    private static Set<Class<? extends Service>> getServiceInterfaces(Object service) {
        Class<?> serviceType = service.getClass();

        Set<Class<? extends Service>> interfaces = new HashSet<>();

        for (Class<?> type : serviceType.getInterfaces()) {
            if (!Service.class.equals(type) && Service.class.isAssignableFrom(type)) {
                interfaces.add(type.asSubclass(Service.class));
            }
        }

        return interfaces;
    }

    @Override
    public String toString() {
        return ToString.format(ConfigurationContext.class, this);
    }
}
