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

package io.hekate.metrics.internal;

import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricConfigBase;
import io.hekate.metrics.MetricsListener;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.MetricsServiceFactory;
import io.hekate.metrics.ProbeConfig;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMetricsService implements MetricsService, InitializingService, TerminatingService {
    private static final Logger log = LoggerFactory.getLogger(DefaultMetricsService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final long refreshInterval;

    @ToStringIgnore
    private final List<MetricsListener> initListeners = new ArrayList<>();

    @ToStringIgnore
    private final List<MetricConfigBase<?>> metricsConfig = new ArrayList<>();

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(MetricsService.class);

    @ToStringIgnore
    private final Map<String, Metric> allMetrics = new HashMap<>();

    @ToStringIgnore
    private final Map<String, DefaultCounterMetric> counters = new HashMap<>();

    @ToStringIgnore
    private final Map<String, DefaultProbeMetric> probes = new HashMap<>();

    @ToStringIgnore
    private final List<MetricsListener> listeners = new CopyOnWriteArrayList<>();

    @ToStringIgnore
    private final AtomicInteger eventSeq = new AtomicInteger();

    @ToStringIgnore
    private ScheduledExecutorService worker;

    public DefaultMetricsService(MetricsServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(MetricsServiceFactory.class);

        check.positive(factory.getRefreshInterval(), "refresh interval");

        refreshInterval = factory.getRefreshInterval();

        Utils.nullSafe(factory.getMetrics()).forEach(metricsConfig::add);

        Utils.nullSafe(new JvmMetricsProvider().getMetricsConfig()).forEach(metricsConfig::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.getMetricsConfig()).forEach(metricsConfig::add)
        );

        Utils.nullSafe(factory.getListeners()).forEach(initListeners::add);
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        guard.lockWrite();

        try {
            guard.becomeInitialized();

            if (DEBUG) {
                log.debug("Initializing...");
            }

            eventSeq.set(0);

            metricsConfig.forEach(this::registerMetric);

            listeners.addAll(initListeners);

            worker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("Metrics"));

            worker.scheduleAtFixedRate(() -> {
                try {
                    updateMetrics();
                } catch (RuntimeException | Error e) {
                    log.error("Got an unexpected runtime error while updating and publishing metrics.", e);
                }
            }, refreshInterval, refreshInterval, TimeUnit.MILLISECONDS);

            if (DEBUG) {
                log.debug("Initialized.");
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void terminate() throws HekateException {
        Waiting waiting = null;

        guard.lockWrite();

        try {
            if (guard.becomeTerminated()) {
                if (DEBUG) {
                    log.debug("Terminating...");
                }

                if (worker != null) {
                    waiting = Utils.shutdown(worker);

                    worker = null;
                }

                allMetrics.clear();
                counters.clear();
                probes.clear();
                listeners.clear();

                eventSeq.set(0);
            }
        } finally {
            guard.unlockWrite();
        }

        if (waiting != null) {
            waiting.awaitUninterruptedly();

            if (DEBUG) {
                log.debug("Terminated.");
            }
        }
    }

    @Override
    public CounterMetric register(CounterConfig config) {
        ConfigCheck check = ConfigCheck.get(CounterConfig.class);

        check.notNull(config, "configuration");
        check.notEmpty(config.getName(), "name");

        guard.lockWrite();

        try {
            if (DEBUG) {
                log.debug("Registering counter [config={}]", config);
            }

            String name = config.getName().trim();

            DefaultCounterMetric existing = counters.get(name);

            if (existing == null) {
                check.unique(name, allMetrics.keySet(), "metric name");

                String totalName = config.getTotalName();

                if (totalName != null) {
                    totalName = totalName.trim();
                }

                if (totalName != null) {
                    check.unique(totalName, allMetrics.keySet(), "metric name");
                }

                DefaultCounterMetric counter = new DefaultCounterMetric(name, config.isAutoReset(), totalName);

                counters.put(name, counter);

                allMetrics.put(name, counter);

                if (totalName != null) {
                    allMetrics.put(totalName, new CounterTotalMetric(counter));
                }

                return counter;
            } else {
                return existing;
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public Metric register(ProbeConfig config) {
        ConfigCheck check = ConfigCheck.get(CounterConfig.class);

        check.notNull(config, "configuration");
        check.notEmpty(config.getName(), "name");
        check.notNull(config.getProbe(), "probe");

        guard.lockWrite();

        try {
            if (DEBUG) {
                log.debug("Registering probe [config={}]", config);
            }

            String name = config.getName().trim();

            check.unique(name, allMetrics.keySet(), "name");

            DefaultProbeMetric metricProbe = new DefaultProbeMetric(name, config.getProbe(), config.getInitValue());

            probes.put(name, metricProbe);

            allMetrics.put(name, metricProbe);

            return metricProbe;
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void addListener(MetricsListener listener) {
        ArgAssert.check(listener != null, "Listener is null.");

        guard.lockReadWithStateCheck();

        try {
            listeners.add(listener);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void removeListener(MetricsListener listener) {
        ArgAssert.check(listener != null, "Listener is null.");

        listeners.remove(listener);
    }

    @Override
    public CounterMetric getCounter(String name) {
        guard.lockReadWithStateCheck();

        try {
            return counters.get(name);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public Map<String, Metric> getAll() {
        guard.lockReadWithStateCheck();

        try {
            return new HashMap<>(allMetrics);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public Metric getMetric(String name) {
        guard.lockReadWithStateCheck();

        try {
            return allMetrics.get(name);
        } finally {
            guard.unlockRead();
        }
    }

    public List<MetricsListener> getListeners() {
        return new ArrayList<>(listeners);
    }

    private void registerMetric(MetricConfigBase<?> metric) {
        if (metric instanceof CounterConfig) {
            register((CounterConfig)metric);
        } else if (metric instanceof ProbeConfig) {
            register((ProbeConfig)metric);
        } else {
            throw new IllegalArgumentException("Unsupported metric type: " + metric);
        }
    }

    private void updateMetrics() {
        Map<String, Metric> snapshot;

        guard.lockWrite();

        try {
            if (guard.isInitialized()) {
                snapshot = !listeners.isEmpty() ? new HashMap<>(allMetrics.size(), 1.0f) : null;

                probes.forEach((name, probe) -> {
                    if (!probe.isFailed()) {
                        try {
                            long newValue = probe.update();

                            if (snapshot != null) {
                                snapshot.put(name, new StaticMetric(name, newValue));
                            }
                        } catch (RuntimeException | Error err) {
                            log.error("Unexpected error while getting the probe value. "
                                + "Probe will not be tried any more [name={}]", name, err);

                            probe.setFailed(true);
                        }
                    }
                });

                counters.values().forEach(c -> {
                    long val;

                    if (c.isAutoReset()) {
                        val = c.reset();
                    } else {
                        val = c.getValue();
                    }

                    if (snapshot != null) {
                        String name = c.getName();

                        snapshot.put(name, new StaticMetric(name, val));

                        if (c.hasTotal()) {
                            String totalName = c.getTotalName();

                            snapshot.put(totalName, new StaticMetric(totalName, c.getTotalValue()));
                        }
                    }
                });
            } else {
                snapshot = null;
            }
        } finally {
            guard.unlockWrite();
        }

        if (snapshot != null) {
            int tick = eventSeq.getAndIncrement();

            DefaultMetricsUpdateEvent event = new DefaultMetricsUpdateEvent(tick, Collections.unmodifiableMap(snapshot));

            listeners.forEach(listener -> {
                try {
                    listener.onUpdate(event);
                } catch (RuntimeException | Error e) {
                    log.error("Failed to notify metrics listener [listener={}]", listener, e);
                }
            });
        }
    }

    @Override
    public String toString() {
        return ToString.format(MetricsService.class, this);
    }
}
