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

package io.hekate.core.service.internal;

import io.hekate.core.Hekate;
import io.hekate.core.HekateConfigurationException;
import io.hekate.core.HekateException;
import io.hekate.core.ServiceInfo;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.Service;
import io.hekate.core.service.ServiceFactory;
import io.micrometer.core.instrument.MeterRegistry;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

public class ServiceManager {
    private static final Logger log = LoggerFactory.getLogger(ServiceManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final Service NULL_SERVICE = new Service() {
        // No-op.
    };

    private final String nodeName;

    private final String clusterName;

    private final Hekate container;

    private final ServiceInitOrder initOrder = new ServiceInitOrder();

    private final List<ServiceHandler> handlers = new ArrayList<>();

    private final Map<Class<? extends Service>, Service> lookupCache = new HashMap<>();

    private final StampedLock lookupLock = new StampedLock();

    private final List<? extends Service> builtInServices;

    private final List<ServiceFactory<?>> factories;

    private final List<Class<? extends Service>> coreServices;

    private final MeterRegistry metrics;

    private Map<String, ServiceInfo> servicesInfo;

    private Set<Class<? extends Service>> serviceTypes;

    public ServiceManager(
        String nodeName,
        String clusterName,
        Hekate container,
        MeterRegistry metrics,
        List<? extends Service> builtInServices,
        List<Class<? extends Service>> coreServices,
        List<? extends ServiceFactory<?>> factories
    ) {
        assert container != null : "Container is null.";
        assert metrics != null : "Metrics registry is null.";
        assert builtInServices != null : "Built-in services list is null.";
        assert coreServices != null : "Core services list is null.";
        assert factories != null : "Service factories list is null.";

        this.nodeName = nodeName;
        this.clusterName = clusterName;
        this.container = container;
        this.metrics = metrics;
        this.builtInServices = builtInServices;
        this.coreServices = coreServices;
        this.factories = new ArrayList<>(factories);
    }

    public String nodeName() {
        return nodeName;
    }

    public String clusterName() {
        return clusterName;
    }

    public Hekate container() {
        return container;
    }

    public void instantiate() {
        if (DEBUG) {
            log.debug("Instantiating services...");
        }

        // Register built-in services.
        builtInServices.forEach(this::registerService);

        // Instantiate services.
        for (ServiceFactory<? extends Service> factory : factories) {
            if (DEBUG) {
                log.debug("Creating new service [factory={}]", factory);
            }

            registerService(factory.createService());
        }

        // Ensure that all core services are registered.
        coreServices.forEach(this::findOrCreateService);

        // Resolve dependencies.
        ServiceDependencyContext depCtx = new ServiceDependencyContext(this);

        // Use index-based iteration since new handlers can be added dynamically during dependencies resolution.
        for (int i = 0; i < handlers.size(); i++) {
            ServiceHandler handler = handlers.get(i);

            handler.resolve(depCtx);
        }

        // Make sure that all services are registered to initialization order.
        handlers.forEach(initOrder::register);

        // Configure services.
        ServiceConfigurationContext cfgCtx = new ServiceConfigurationContext(container, this);

        handlers.forEach(handler ->
            handler.configure(cfgCtx)
        );

        servicesInfo = unmodifiableMap(cfgCtx.servicesInfo());

        serviceTypes = unmodifiableSet(depCtx.serviceTypes());

        if (DEBUG) {
            log.debug("Instantiated services.");
        }
    }

    public void preInitialize(InitializationContext ctx) throws HekateException {
        assert ctx != null : "Initialization context is null.";

        if (DEBUG) {
            log.debug("Pre-initializing services [context={}]", ctx);
        }

        // Initialize services.
        for (ServiceHandler handler : initOrder.order()) {
            handler.preInitialize(ctx);
        }

        if (DEBUG) {
            log.debug("Pre-initialized services.");
        }
    }

    public void initialize(InitializationContext ctx) throws HekateException {
        assert ctx != null : "Initialization context is null.";

        if (DEBUG) {
            log.debug("Initializing services [context={}]", ctx);
        }

        // Initialize services.
        for (ServiceHandler handler : initOrder.order()) {
            handler.initialize(ctx);
        }

        if (DEBUG) {
            log.debug("Initialized services.");
        }
    }

    public void postInitialize(InitializationContext ctx) throws HekateException {
        assert ctx != null : "Initialization context is null.";

        if (DEBUG) {
            log.debug("Post-initializing services [context={}]", ctx);
        }

        // Initialize services.
        for (ServiceHandler handler : initOrder.order()) {
            handler.postInitialize(ctx);
        }

        if (DEBUG) {
            log.debug("Post-initialized services.");
        }
    }

    public void preTerminate() {
        if (DEBUG) {
            log.debug("Pre-terminating services...");
        }

        List<ServiceHandler> order = initOrder.order();

        // Terminate in reversed order.
        for (int i = order.size() - 1; i >= 0; i--) {
            order.get(i).preTerminate();
        }

        if (DEBUG) {
            log.debug("Pre-terminated services.");
        }
    }

    public void terminate() {
        if (DEBUG) {
            log.debug("Terminating services...");
        }

        List<ServiceHandler> order = initOrder.order();

        // Terminate in reversed order.
        for (int i = order.size() - 1; i >= 0; i--) {
            order.get(i).terminate();
        }

        if (DEBUG) {
            log.debug("Terminated services.");
        }
    }

    public void postTerminate() {
        if (DEBUG) {
            log.debug("Post-terminating services...");
        }

        List<ServiceHandler> order = initOrder.order();

        // Terminate in reversed order.
        for (int i = order.size() - 1; i >= 0; i--) {
            order.get(i).postTerminate();
        }

        if (DEBUG) {
            log.debug("Post-terminated services.");
        }
    }

    public <T extends Service> T findService(Class<T> type) {
        assert type != null : "Service type is null.";

        Service service;

        // Try to find with the read lock.
        long readLock = lookupLock.readLock();

        try {
            service = lookupCache.get(type);
        } finally {
            lookupLock.unlock(readLock);
        }

        if (service == null) {
            // Try to resolve service while holding the write lock.
            long writeLock = lookupLock.writeLock();

            try {
                // Double check that service was not registered while we were obtaining the write lock.
                service = lookupCache.get(type);

                if (service == null) {
                    ServiceHandler handler = findServiceDirect(type);

                    if (handler == null) {
                        service = NULL_SERVICE;
                    } else {
                        service = handler.service();
                    }

                    lookupCache.put(type, service);
                }
            } finally {
                lookupLock.unlock(writeLock);
            }

        }

        return service != NULL_SERVICE ? type.cast(service) : null;
    }

    public Map<String, ServiceInfo> getServicesInfo() {
        return servicesInfo;
    }

    public Set<Class<? extends Service>> getServiceTypes() {
        return serviceTypes;
    }

    public List<ServiceHandler> getHandlers() {
        return handlers;
    }

    public MeterRegistry metrics() {
        return metrics;
    }

    ServiceHandler findServiceDirect(Class<? extends Service> type) {
        for (ServiceHandler handler : handlers) {
            Object service = handler.service();

            if (type.isAssignableFrom(service.getClass())) {
                return handler;
            }
        }

        return null;
    }

    ServiceHandler findOrCreateService(Class<? extends Service> type) {
        ServiceHandler handler = findServiceDirect(type);

        if (handler == null && type.isAnnotationPresent(DefaultServiceFactory.class)) {
            Class<? extends ServiceFactory<?>> factoryType = type.getAnnotation(DefaultServiceFactory.class).value();

            if (DEBUG) {
                log.debug("Instantiating service with default factory [type={}, factory={}]", type.getName(), factoryType.getName());
            }

            try {
                ServiceFactory<?> factory = factoryType.getConstructor().newInstance();

                Service service = factory.createService();

                if (!type.isAssignableFrom(service.getClass())) {
                    throw new HekateConfigurationException("Invalid usage of @" + DefaultServiceFactory.class.getName() + " annotation. "
                        + "Service factory was expected to create an instance of " + type.getName() + " but created an instance of "
                        + service.getClass().getName());
                }

                handler = registerService(service);
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new HekateConfigurationException("Failed to instantiate service with default factory "
                    + "[service=" + type.getName() + ", factory=" + factoryType.getName() + ']', e);
            }
        }

        return handler;
    }

    private ServiceHandler registerService(Service service) {
        ServiceHandler handler = new ServiceHandler(service, initOrder);

        handlers.add(handler);

        return handler;
    }
}
