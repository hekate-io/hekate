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

package io.hekate.coordinate;

import io.hekate.coordinate.internal.DefaultCoordinationService;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link CoordinationService}.
 *
 * <p>
 * This class represents a configurable factory for {@link CoordinationService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link CoordinationService}.
 * </p>
 *
 * <p>
 * For more details about the {@link CoordinationService} and its capabilities please see the documentation of {@link CoordinationService}
 * interface.
 * </p>
 *
 * @see CoordinationService
 */
public class CoordinationServiceFactory implements ServiceFactory<CoordinationService> {
    /** Default value in milliseconds (={@value}) for {@link #setRetryInterval(long)}. */
    public static final int DEFAULT_RETRY_INTERVAL = 50;

    /** Default value in milliseconds (={@value}) for {@link #setIdleSocketTimeout(long)}. */
    public static final long DEFAULT_IDLE_SOCKET_TIMEOUT = 60_000;

    /** See {@link #setNioThreads(int)}. */
    private int nioThreads;

    /** See {@link #setRetryInterval(long)}. */
    private long retryInterval = DEFAULT_RETRY_INTERVAL;

    /** See {@link #setProcesses(List)}. */
    private List<CoordinationProcessConfig> processes;

    /** See {@link #setConfigProviders(List)}. */
    private List<CoordinationConfigProvider> configProviders;

    /** See {@link #setIdleSocketTimeout(long)}. */
    private long idleSocketTimeout = DEFAULT_IDLE_SOCKET_TIMEOUT;

    /**
     * Returns the size of a thread pool for handling NIO-based socket connections.
     *
     * @return Size of a thread pool for handling NIO-based socket connections.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the size of a thread pool for handling NIO-based socket connections.
     *
     * <p>
     * If this parameter is less than or equals to zero (default value) then this channel will use the core thread pool of
     * {@link NetworkService} (see {@link NetworkServiceFactory#setNioThreads(int)}).
     * </p>
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads Size of a thread pool for handling NIO-based socket connections.
     *
     * @return This instance.
     */
    public CoordinationServiceFactory withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    /**
     * Returns the time interval in milliseconds between retry attempts in case of network communication failures (see {@link
     * #setRetryInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getRetryInterval() {
        return retryInterval;
    }

    /**
     * Sets the time interval in milliseconds between retry attempts in case of network communication failures.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_RETRY_INTERVAL}.
     * </p>
     *
     * @param retryInterval Time interval in milliseconds.
     */
    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    /**
     * Fluent-style version of {@link #setRetryInterval(long)}.
     *
     * @param retryInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public CoordinationServiceFactory withRetryInterval(long retryInterval) {
        setRetryInterval(retryInterval);

        return this;
    }

    /**
     * Returns the list of coordination process configurations that should be automatically registered during the coordination service
     * startup (see {@link #setProcesses(List)}).
     *
     * @return Coordination processes configuration.
     */
    public List<CoordinationProcessConfig> getProcesses() {
        return processes;
    }

    /**
     * Sets the list of coordination process configurations that should be automatically registered during the coordination service
     * startup.
     *
     * @param processes Coordination processes configuration..
     */
    public void setProcesses(List<CoordinationProcessConfig> processes) {
        this.processes = processes;
    }

    /**
     * Fluent-style version of {@link #setProcesses(List)}.
     *
     * @param process Coordination process configuration.
     *
     * @return This instance.
     */
    public CoordinationServiceFactory withProcess(CoordinationProcessConfig process) {
        if (processes == null) {
            processes = new ArrayList<>();
        }

        processes.add(process);

        return this;
    }

    /**
     * Fluent-style shortcut to register a new {@link CoordinationProcessConfig} with the specified
     * {@link CoordinationProcessConfig#setName(String) name}.
     *
     * @param name Process name (see {@link CoordinationProcessConfig#setName(String)}).
     *
     * @return New coordination process configuration.
     */
    public CoordinationProcessConfig withProcess(String name) {
        CoordinationProcessConfig process = new CoordinationProcessConfig(name);

        withProcess(process);

        return process;
    }

    /**
     * Returns the list of coordination process configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Coordination process configuration providers.
     */
    public List<CoordinationConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of coordination process configuration providers.
     *
     * @param configProviders Coordination process configuration providers.
     *
     * @see CoordinationConfigProvider
     */
    public void setConfigProviders(List<CoordinationConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Coordination process configuration provider.
     *
     * @return This instance.
     */
    public CoordinationServiceFactory withConfigProvider(CoordinationConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    /**
     * Returns the idle socket timeout in milliseconds (see {@link #setIdleSocketTimeout(long)}).
     *
     * @return Idle socket timeout in milliseconds.
     */
    public long getIdleSocketTimeout() {
        return idleSocketTimeout;
    }

    /**
     * Sets idle socket timeout in milliseconds.
     *
     * <p>
     * If there were no communication with some remote node for the duration of this time interval then all sockets connections with such
     * node will be closed in order to save system resource. Connections will be automatically reestablish on the next attempt to send a
     * message to that node.
     * </p>
     *
     * <p>
     * If value of this parameter is less than or equals to zero then connections will not be closed while remote node stays
     * alive.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_IDLE_SOCKET_TIMEOUT}.
     * </p>
     *
     * @param idleSocketTimeout Timeout in milliseconds.
     */
    public void setIdleSocketTimeout(long idleSocketTimeout) {
        this.idleSocketTimeout = idleSocketTimeout;
    }

    /**
     * Fluent-style version of {@link #setIdleSocketTimeout(long)}.
     *
     * @param idleTimeout Timeout in milliseconds.
     *
     * @return This instance.
     */
    public CoordinationServiceFactory withIdleSocketTimeout(long idleTimeout) {
        setIdleSocketTimeout(idleTimeout);

        return this;
    }

    @Override
    public CoordinationService createService() {
        return new DefaultCoordinationService(this);
    }
}
