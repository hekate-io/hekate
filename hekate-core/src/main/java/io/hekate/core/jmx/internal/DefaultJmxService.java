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

package io.hekate.core.jmx.internal;

import io.hekate.core.HekateException;
import io.hekate.core.HekateUncheckedException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceException;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.core.jmx.JmxSupport;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import javax.management.InstanceAlreadyExistsException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MXBean;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.core.jmx.internal.JmxUtils.jmxName;
import static java.util.stream.Collectors.toList;

public class DefaultJmxService implements JmxService, InitializingService, TerminatingService {
    private static final Logger log = LoggerFactory.getLogger(DefaultJmxService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String domain;

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(JmxService.class);

    @ToStringIgnore
    private final MBeanServer server;

    @ToStringIgnore
    private final List<ObjectName> names = new ArrayList<>();

    public DefaultJmxService(JmxServiceFactory factory) {
        assert factory != null : "Service factory is null.";

        server = factory.getServer();
        domain = factory.getDomain();

        ConfigCheck check = ConfigCheck.get(JmxServiceFactory.class);

        check.notNull(server, "server");
        check.notEmpty(domain, "domain");
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void preTerminate() throws HekateException {
        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating...");
                }

                for (ObjectName jmxName : names) {
                    if (server.isRegistered(jmxName)) {
                        if (DEBUG) {
                            log.debug("Unregister JMX MBean [name={}]", jmxName);
                        }

                        try {
                            server.unregisterMBean(jmxName);
                        } catch (InstanceNotFoundException | MBeanRegistrationException e) {
                            log.warn("Failed to unregister JMX bean [name={}]", jmxName, e);
                        }
                    }
                }

                names.clear();

                if (DEBUG) {
                    log.debug("Terminated.");
                }
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() throws HekateException {
        // Actual un-registration of JMX beans happens in preTerminate(...) method.
        // Need to do it in order to make sure that JMX beans are unregistered before
        // their corresponding services get terminated.
    }

    @Override
    public String domain() {
        return domain;
    }

    @Override
    public List<ObjectName> names() {
        guard.lockRead();

        try {
            return new ArrayList<>(names);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public ObjectName nameFor(Class<?> jmxInterface) {
        return nameFor(jmxInterface, null);
    }

    @Override
    public ObjectName nameFor(Class<?> jmxInterface, String nameAttribute) {
        try {
            return jmxName(domain, jmxInterface, nameAttribute);
        } catch (MalformedObjectNameException e) {
            // Should not happen.
            throw new HekateUncheckedException("Failed to construct JMX object name.", e);
        }
    }

    @Override
    public Optional<ObjectName> register(Object mxBean) throws JmxServiceException {
        return register(mxBean, null);
    }

    @Override
    public Optional<ObjectName> register(Object mxBean, String nameAttribute) throws JmxServiceException {
        ArgAssert.notNull(mxBean, "JMX bean");

        // Resolve real JMX object.
        Object realMxBean;

        if (mxBean instanceof JmxSupport) {
            realMxBean = ((JmxSupport)mxBean).jmx();
        } else {
            realMxBean = mxBean;
        }

        if (realMxBean instanceof Collection) {
            // Register all JMX objects from the supplied collection.
            for (Object nested : (Collection)realMxBean) {
                if (nested != null) {
                    register(nested, nameAttribute);
                }
            }

            return Optional.empty();
        } else {
            // Register JMX object.
            guard.lockWriteWithStateCheck();

            try {
                // Collect all JMX interfaces.
                List<Class<?>> faces = Stream.of(realMxBean.getClass().getInterfaces())
                    .filter(it -> it.isAnnotationPresent(MXBean.class))
                    .collect(toList());

                // Make sure that only one JMX interface is declared by the JMX object.
                if (faces.size() > 1) {
                    String errMsg = String.format("JMX object implements more than one @%s-annotated interfaces [object=%s, interfaces=%s]",
                        MXBean.class.getSimpleName(), realMxBean, faces);

                    throw new JmxServiceException(errMsg);
                }

                if (faces.isEmpty()) {
                    // Nothing to register.
                    return Optional.empty();
                } else {
                    // Register JMX bean.
                    Class<?> face = faces.get(0);
                    ObjectName name = nameFor(face, nameAttribute);

                    if (DEBUG) {
                        log.debug("Registering JMX bean [name={}]", name);
                    }

                    JmxBeanHandler jmxHandler;

                    try {
                        jmxHandler = new JmxBeanHandler(realMxBean, face, true);
                    } catch (IllegalArgumentException err) {
                        // MXBean introspection failure.
                        String errMsg = String.format("Failed to register JMX bean [name=%s, type=%s]", name, face);

                        throw new JmxServiceException(errMsg, err.getCause() != null ? err.getCause() : err);
                    }

                    try {
                        server.registerMBean(jmxHandler, name);
                    } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException err) {
                        String errMsg = String.format("Failed to register JMX bean [name=%s, type=%s]", name, face);

                        throw new JmxServiceException(errMsg, err);
                    }

                    // Remember the name so that we could unregister this bean during the termination of this service.
                    names.add(name);

                    return Optional.of(name);
                }
            } finally {
                guard.unlockWrite();
            }
        }
    }

    @Override
    public MBeanServer server() {
        return server;
    }

    @Override
    public String toString() {
        return ToString.format(JmxService.class, this);
    }
}
