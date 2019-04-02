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

import io.hekate.core.HekateException;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.Service;
import io.hekate.core.service.TerminatingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ServiceHandler {
    private static final Logger log = LoggerFactory.getLogger(ServiceHandler.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final Service service;

    private final ServiceInitOrder initOrder;

    private boolean resolved;

    private boolean configured;

    public ServiceHandler(Service service, ServiceInitOrder initOrder) {
        assert service != null : "Service is null.";
        assert initOrder != null : "Initialization order handler is null.";

        this.service = service;
        this.initOrder = initOrder;
    }

    public Service service() {
        return service;
    }

    public void resolve(ServiceDependencyContext ctx) {
        if (!resolved) {
            resolved = true;

            ctx.prepare(service);

            try {
                if (service instanceof DependentService) {
                    DependentService dependent = (DependentService)service;

                    if (DEBUG) {
                        log.debug("Resolving service dependencies [service={}]", service);
                    }

                    dependent.resolve(ctx);
                }
            } finally {
                ctx.close();
            }

            initOrder.register(this);
        }
    }

    public void configure(ServiceConfigurationContext ctx) {
        if (!configured) {
            configured = true;

            ctx.prepare(service);

            try {
                if (service instanceof ConfigurableService) {
                    ConfigurableService conf = (ConfigurableService)service;

                    if (DEBUG) {
                        log.debug("Configuring service [service={}]", service);
                    }

                    conf.configure(ctx);
                }
            } finally {
                ctx.close();
            }
        }
    }

    public void preInitialize(InitializationContext ctx) throws HekateException {
        if (service instanceof InitializingService) {
            if (DEBUG) {
                log.debug("Pre-initializing service [service={}]", service);
            }

            InitializingService init = (InitializingService)service;

            init.preInitialize(ctx);
        }
    }

    public void initialize(InitializationContext ctx) throws HekateException {
        if (service instanceof InitializingService) {
            if (DEBUG) {
                log.debug("Initializing service [service={}]", service);
            }

            InitializingService init = (InitializingService)service;

            init.initialize(ctx);
        }

        log.info("Initialized {}", service);
    }

    public void postInitialize(InitializationContext ctx) throws HekateException {
        if (service instanceof InitializingService) {
            if (DEBUG) {
                log.debug("Post-initializing service [service={}]", service);
            }

            InitializingService init = (InitializingService)service;

            init.postInitialize(ctx);
        }
    }

    public void preTerminate() {
        if (service instanceof TerminatingService) {
            if (DEBUG) {
                log.debug("Pre-terminating service [service={}]", service);
            }

            TerminatingService term = (TerminatingService)service;

            try {
                term.preTerminate();
            } catch (HekateException | RuntimeException | Error e) {
                log.error("Failed to pre-terminate service [service={}]", term, e);
            }
        }
    }

    public void terminate() {
        if (service instanceof TerminatingService) {
            if (DEBUG) {
                log.debug("Terminating service [service={}]", service);
            }

            TerminatingService term = (TerminatingService)service;

            try {
                term.terminate();
            } catch (HekateException | RuntimeException | Error e) {
                log.error("Failed to terminate service [service={}]", term, e);
            }
        }

        if (log.isInfoEnabled()) {
            log.info("Terminated {}", service);
        }
    }

    public void postTerminate() {
        if (service instanceof TerminatingService) {
            if (DEBUG) {
                log.debug("Post-terminating service [service={}]", service);
            }

            TerminatingService term = (TerminatingService)service;

            try {
                term.postTerminate();
            } catch (HekateException | RuntimeException | Error e) {
                log.error("Failed to post-terminate service [service={}]", term, e);
            }
        }
    }
}
