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

package io.hekate.core.jmx;

import io.hekate.core.jmx.internal.DefaultJmxService;
import io.hekate.core.service.ServiceFactory;
import io.hekate.util.format.ToString;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Factory for {@link JmxService}.
 */
public class JmxServiceFactory implements ServiceFactory<JmxService> {
    /** Default value ({@value}) for {@link #setDomain(String)}. */
    public static final String DEFAULT_DOMAIN = "io.hekate";

    private String domain = DEFAULT_DOMAIN;

    private MBeanServer server = defaultServer();

    @Override
    public JmxService createService() {
        return new DefaultJmxService(this);
    }

    /**
     * Returns the JMX domain (see {@link #setDomain(String)}).
     *
     * @return JMX domain.
     */
    public String getDomain() {
        return domain;
    }

    /**
     * Sets the JMX domain.
     *
     * <p>
     * Value of this parameter is used as a JMX domain name when constructing {@link ObjectName}s for JMX components.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_DOMAIN}.
     * </p>
     *
     * @param domain JMX domain.
     */
    public void setDomain(String domain) {
        this.domain = domain;
    }

    /**
     * Fluent-style version of {@link #setDomain(String)}.
     *
     * @param domain JMX domain.
     *
     * @return This instance.         T
     */
    public JmxServiceFactory withDomain(String domain) {
        setDomain(domain);

        return this;
    }

    /**
     * Returns the MBean server (see {@link MBeanServer}).
     *
     * @return MBean server.
     */
    public MBeanServer getServer() {
        return server;
    }

    /**
     * Sets the MBean server that should be used by the {@link JmxService} to register JMX beans.
     *
     * <p>
     * This parameter is optional and if not specified then the {@link ManagementFactory#getPlatformMBeanServer()} will be used by default.
     * </p>
     *
     * @param server MBean server.
     */
    public void setServer(MBeanServer server) {
        this.server = server == null ? defaultServer() : server;
    }

    /**
     * Fluent-style version of {@link #setServer(MBeanServer)}.
     *
     * @param server MBean server.
     *
     * @return This instance.
     */
    public JmxServiceFactory withServer(MBeanServer server) {
        setServer(server);

        return this;
    }

    private static MBeanServer defaultServer() {
        return ManagementFactory.getPlatformMBeanServer();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
