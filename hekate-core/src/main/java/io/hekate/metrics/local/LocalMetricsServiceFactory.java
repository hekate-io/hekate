/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.metrics.local;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.metrics.local.internal.DefaultLocalMetricsService;
import io.hekate.util.format.ToString;
import io.hekate.util.time.SystemTimeSupplier;
import java.util.ArrayList;
import java.util.List;

/**
 * Factory for {@link LocalMetricsService}.
 *
 * <p>
 * This class represents a configurable factory for {@link LocalMetricsService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link LocalMetricsService}.
 * </p>
 *
 * <p>
 * For more details about the {@link LocalMetricsService} and its capabilities please see the documentation of {@link LocalMetricsService}
 * interface.
 * </p>
 *
 * @see LocalMetricsService
 */
public class LocalMetricsServiceFactory implements ServiceFactory<LocalMetricsService> {
    /** Default value (={@value}) in milliseconds for {@link #setRefreshInterval(long)}. */
    public static final int DEFAULT_REFRESH_INTERVAL = 1000;

    private long refreshInterval = DEFAULT_REFRESH_INTERVAL;

    private List<MetricConfigBase<?>> metrics;

    private List<MetricsConfigProvider> configProviders;

    private List<MetricsListener> listeners;

    private SystemTimeSupplier systemTime;

    /**
     * Returns the time interval in milliseconds to poll for metric changes and {@link #setListeners(List) listeners} notification (see
     * {@link #setRefreshInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Sets the time interval in milliseconds to poll for metric changes and {@link #setListeners(List) listeners} notification.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_REFRESH_INTERVAL}.
     * </p>
     *
     * @param refreshInterval Time interval in milliseconds.
     */
    public void setRefreshInterval(long refreshInterval) {
        this.refreshInterval = refreshInterval;
    }

    /**
     * Fluent-style version of {@link #setRefreshInterval(long)}.
     *
     * @param refreshInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public LocalMetricsServiceFactory withRefreshInterval(long refreshInterval) {
        setRefreshInterval(refreshInterval);

        return this;
    }

    /**
     * Returns the list of metrics that should be automatically registered during the metrics service startup (see {@link
     * #setMetrics(List)}).
     *
     * @return List of metrics.
     */
    public List<MetricConfigBase<?>> getMetrics() {
        return metrics;
    }

    /**
     * Sets the list of metrics that should be automatically registered during the metrics service startup.
     *
     * @param metrics Metrics to be registered.
     *
     * @see LocalMetricsService#register(CounterConfig)
     */
    public void setMetrics(List<MetricConfigBase<?>> metrics) {
        this.metrics = metrics;
    }

    /**
     * Fluent-style version of {@link #setMetrics(List)}.
     *
     * @param metric Metric to be registered.
     *
     * @return This instance.
     */
    public LocalMetricsServiceFactory withMetric(MetricConfigBase<?> metric) {
        if (metrics == null) {
            metrics = new ArrayList<>();
        }

        metrics.add(metric);

        return this;
    }

    /**
     * Returns the list of metrics configuration providers (see {@link #setConfigProviders(List)}).
     *
     * @return Metrics configuration providers.
     */
    public List<MetricsConfigProvider> getConfigProviders() {
        return configProviders;
    }

    /**
     * Sets the list of metrics configuration providers.
     *
     * @param configProviders Metrics configuration providers.
     *
     * @see MetricsConfigProvider
     */
    public void setConfigProviders(List<MetricsConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Fluent-style version of {@link #setConfigProviders(List)}.
     *
     * @param configProvider Metrics configuration provider.
     *
     * @return This instance.
     */
    public LocalMetricsServiceFactory withConfigProvider(MetricsConfigProvider configProvider) {
        if (configProviders == null) {
            configProviders = new ArrayList<>();
        }

        configProviders.add(configProvider);

        return this;
    }

    /**
     * Returns the list of listeners that should be automatically registered during the metrics service startup (see {@link
     * #setListeners(List)}).
     *
     * @return List of listeners.
     */
    public List<MetricsListener> getListeners() {
        return listeners;
    }

    /**
     * Sets the list of listeners that should be automatically registered during the metrics service startup.
     *
     * @param listeners Listeners to be registered.
     *
     * @see LocalMetricsService#addListener(MetricsListener)
     */
    public void setListeners(List<MetricsListener> listeners) {
        this.listeners = listeners;
    }

    /**
     * Fluent-style version of {@link #setListeners(List)}.
     *
     * @param listener Listener to be registered.
     *
     * @return This instance.
     */
    public LocalMetricsServiceFactory withListener(MetricsListener listener) {
        if (listeners == null) {
            listeners = new ArrayList<>();
        }

        listeners.add(listener);

        return this;
    }

    /**
     * Returns the system time supplier for time-based metrics (see {@link #setSystemTime(SystemTimeSupplier)}).
     *
     * @return System time supplier.
     */
    public SystemTimeSupplier getSystemTime() {
        return systemTime;
    }

    /**
     * Returns the system time supplier for time-based metrics.
     *
     * <p>
     * This property is optional and is intended to be used for testing purposes for system time emulation.
     * </p>
     *
     * @param systemTime System time supplier.
     */
    public void setSystemTime(SystemTimeSupplier systemTime) {
        this.systemTime = systemTime;
    }

    /**
     * Fluent-style version of {@link #setSystemTime(SystemTimeSupplier)}.
     *
     * @param systemTime System time supplier.
     *
     * @return This instance.
     */
    public LocalMetricsServiceFactory withSystemTime(SystemTimeSupplier systemTime) {
        setSystemTime(systemTime);

        return this;
    }

    @Override
    public LocalMetricsService createService() {
        return new DefaultLocalMetricsService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
