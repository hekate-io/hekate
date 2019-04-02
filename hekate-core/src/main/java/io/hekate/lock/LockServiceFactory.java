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

package io.hekate.lock;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.lock.internal.DefaultLockService;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.util.format.ToString;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link LockService}.
 *
 * <p>
 * This class represents a configurable factory for {@link LockService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link LockService}.
 * </p>
 *
 * <p>
 * For more details about the {@link LockService} and its capabilities please see the documentation of {@link LockService} interface.
 * </p>
 */
public class LockServiceFactory implements ServiceFactory<LockService> {
    /** Default value in milliseconds (={@value}) for {@link #setRetryInterval(long)}. */
    public static final int DEFAULT_RETRY_INTERVAL = 50;

    /** Default value (={@value}) for {@link #setWorkerThreads(int)}. */
    public static final int DEFAULT_WORKER_THREADS = 1;

    private long retryInterval = DEFAULT_RETRY_INTERVAL;

    private int workerThreads = DEFAULT_WORKER_THREADS;

    private int nioThreads;

    private List<LockRegionConfig> regions;

    private List<LockConfigProvider> configProviders;

    /**
     * Returns the list of lock region configurations (see {@link #setRegions(List)}).
     *
     * @return Lock region configurations.
     */
    public List<LockRegionConfig> getRegions() {
        return regions;
    }

    /**
     * Sets the list of lock region configurations that should be registered to the {@link LockService}.
     *
     * @param regions Lock regions configuration.
     */
    public void setRegions(List<LockRegionConfig> regions) {
        this.regions = regions;
    }

    /**
     * Fluent-style version of {@link #setRegions(List)}.
     *
     * @param region Lock region configuration that should be registered to the {@link LockService}.
     *
     * @return This instance.
     */
    public LockServiceFactory withRegion(LockRegionConfig region) {
        if (regions == null) {
            regions = new ArrayList<>();
        }

        regions.add(region);

        return this;
    }

    /**
     * Returns the list of lock region configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Lock region configuration providers.
     */
    public List<LockConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of lock region configuration providers.
     *
     * @param configProviders Lock region configuration providers.
     *
     * @see LockConfigProvider
     */
    public void setConfigProviders(List<LockConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Lock region configuration provider.
     *
     * @return This instance.
     */
    public LockServiceFactory withConfigProvider(LockConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

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
    public LockServiceFactory withRetryInterval(long retryInterval) {
        setRetryInterval(retryInterval);

        return this;
    }

    /**
     * Returns the size of a thread pool that will perform locks management operations (see {@link #setWorkerThreads(int)}).
     *
     * @return Worker thread pool size.
     */
    public int getWorkerThreads() {
        return workerThreads;
    }

    /**
     * Sets the side of a thread pool that will perform lock management operations.
     *
     * <p>
     * Thread pool of this size is used by the {@link LockService} to perform in-memory lock operations.
     * </p>
     *
     * <p>
     * Value of this parameter must be equals or greater than 1. Default value is {@value DEFAULT_WORKER_THREADS}.
     * </p>
     *
     * @param workerThreads Worker thread pool size.
     */
    public void setWorkerThreads(int workerThreads) {
        this.workerThreads = workerThreads;
    }

    /**
     * Fluent-style version of {@link #setWorkerThreads(int)}.
     *
     * @param workerThreads Worker thread pool size.
     *
     * @return This instance.
     */
    public LockServiceFactory withWorkerThreads(int workerThreads) {
        setWorkerThreads(workerThreads);

        return this;
    }

    /**
     * Returns the size of a thread pool for handling NIO-based network communications (see {@link #setNioThreads(int)}).
     *
     * @return NIO thread pool size.
     */
    public int getNioThreads() {
        return nioThreads;
    }

    /**
     * Sets the size of a thread pool for handling NIO-based network communications.
     *
     * <p>
     * If value of this parameter is above zero then a dedicated thread pool will be used for handling NIO-based network communications.
     * If this parameter is less than or equals to zero (default value) then the core thread pool of a {@link NetworkService} will be used
     * (see {@link NetworkServiceFactory#setNioThreads(int)}).
     * </p>
     *
     * @param nioThreads NIO thread pool size.
     */
    public void setNioThreads(int nioThreads) {
        this.nioThreads = nioThreads;
    }

    /**
     * Fluent-style version of {@link #setNioThreads(int)}.
     *
     * @param nioThreads NIO thread pool size.
     *
     * @return This instance.
     */
    public LockServiceFactory withNioThreads(int nioThreads) {
        setNioThreads(nioThreads);

        return this;
    }

    @Override
    public LockService createService() {
        return new DefaultLockService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
