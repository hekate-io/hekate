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

package io.hekate.metrics.local.internal;

import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.internal.util.Waiting;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.MetricConfigBase;
import io.hekate.metrics.local.MetricsListener;
import io.hekate.metrics.local.ProbeConfig;
import io.hekate.util.StateGuard;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultLocalMetricsService implements LocalMetricsService, InitializingService, TerminatingService {
    private static final Logger log = LoggerFactory.getLogger(DefaultLocalMetricsService.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final ConfigCheck COUNTER_CHECK = ConfigCheck.get(CounterConfig.class);

    private static final ConfigCheck PROBE_CHECK = ConfigCheck.get(ProbeConfig.class);

    private final long refreshInterval;

    @ToStringIgnore
    private final List<MetricsListener> initListeners = new ArrayList<>();

    @ToStringIgnore
    private final List<MetricConfigBase<?>> metricsConfig = new ArrayList<>();

    @ToStringIgnore
    private final StateGuard guard = new StateGuard(LocalMetricsService.class);

    @ToStringIgnore
    private final Map<String, Metric> allMetrics = new HashMap<>();

    @ToStringIgnore
    private final Map<String, DefaultCounterMetric> counters = new HashMap<>();

    @ToStringIgnore
    private final Map<String, DefaultProbeMetric> probes = new HashMap<>();

    @ToStringIgnore
    private final List<MetricsListener> listeners = new CopyOnWriteArrayList<>();

    @ToStringIgnore
    private final AtomicInteger tickSeq = new AtomicInteger();

    @ToStringIgnore
    private ScheduledExecutorService worker;

    public DefaultLocalMetricsService(LocalMetricsServiceFactory factory) {
        assert factory != null : "Factory is null.";

        ConfigCheck check = ConfigCheck.get(LocalMetricsServiceFactory.class);

        check.positive(factory.getRefreshInterval(), "refresh interval");

        refreshInterval = factory.getRefreshInterval();

        Utils.nullSafe(factory.getMetrics()).forEach(metricsConfig::add);

        Utils.nullSafe(new JvmMetricsProvider().configureMetrics()).forEach(metricsConfig::add);

        Utils.nullSafe(factory.getConfigProviders()).forEach(provider ->
            Utils.nullSafe(provider.configureMetrics()).forEach(metricsConfig::add)
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

            tickSeq.set(0);

            registerMetrics();

            listeners.addAll(initListeners);

            worker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("LocalMetrics"));

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

                tickSeq.set(0);
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
    public CounterMetric register(CounterConfig cfg) {
        String name = checkCounterConfig(cfg);

        // Check for an existing counter.
        guard.lockRead();

        try {
            CounterMetric existing = counters.get(name);

            if (existing != null) {
                return existing;
            }
        } finally {
            guard.unlockRead();
        }

        guard.lockWrite();

        try {
            // Double check that counter wasn't registered while we were waiting for the write lock.
            CounterMetric existing = counters.get(name);

            if (existing == null) {
                return doRegister(name, cfg);
            } else {
                return existing;
            }
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public Metric register(ProbeConfig cfg) {
        String name = checkProbeConfig(cfg);

        guard.lockWrite();

        try {
            return doRegister(name, cfg);
        } finally {
            guard.unlockWrite();
        }
    }

    @Override
    public void addListener(MetricsListener listener) {
        ArgAssert.notNull(listener, "Listener");

        guard.lockReadWithStateCheck();

        try {
            listeners.add(listener);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public void removeListener(MetricsListener listener) {
        ArgAssert.notNull(listener, "Listener");

        listeners.remove(listener);
    }

    @Override
    public CounterMetric counter(String name) {
        guard.lockReadWithStateCheck();

        try {
            return counters.get(name);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public Map<String, Metric> allMetrics() {
        guard.lockReadWithStateCheck();

        try {
            return new HashMap<>(allMetrics);
        } finally {
            guard.unlockRead();
        }
    }

    @Override
    public Metric metric(String name) {
        guard.lockReadWithStateCheck();

        try {
            return allMetrics.get(name);
        } finally {
            guard.unlockRead();
        }
    }

    /**
     * Returns all registered {@link #addListener(MetricsListener) listeners}.
     *
     * @return Listeners.
     */
    public List<MetricsListener> listeners() {
        return new ArrayList<>(listeners);
    }

    private void registerMetrics() {
        Map<String, CounterConfig> countersCfg = new HashMap<>();
        Map<String, ProbeConfig> probesCfg = new HashMap<>();

        metricsConfig.forEach(cfg -> {
            if (cfg instanceof CounterConfig) {
                CounterConfig newCfg = (CounterConfig)cfg;

                String name = checkCounterConfig(newCfg);

                CounterConfig oldCfg = countersCfg.get(name);

                if (oldCfg == null) {
                    countersCfg.put(name, newCfg);
                } else {
                    oldCfg.setAutoReset(oldCfg.isAutoReset() | newCfg.isAutoReset());

                    String oldTotal = Utils.nullOrTrim(oldCfg.getTotalName());
                    String newTotal = Utils.nullOrTrim(newCfg.getTotalName());

                    if (newTotal != null) {
                        if (oldTotal == null) {
                            oldCfg.setTotalName(newTotal);
                        } else {
                            COUNTER_CHECK.isTrue(Objects.equals(oldTotal, newTotal),
                                "couldn't merge configuration of a counter metric with different 'total' names "
                                    + "[counter=" + name
                                    + ", total-name-1=" + oldTotal
                                    + ", total-name-2=" + newTotal
                                    + ']');
                        }
                    }
                }
            } else if (cfg instanceof ProbeConfig) {
                ProbeConfig newCfg = (ProbeConfig)cfg;

                String name = checkProbeConfig(newCfg);

                ProbeConfig oldCfg = probesCfg.get(name);

                if (oldCfg == null) {
                    probesCfg.put(name, newCfg);
                } else {
                    oldCfg.setInitValue(Math.max(oldCfg.getInitValue(), newCfg.getInitValue()));

                    PROBE_CHECK.isTrue(Objects.equals(oldCfg.getProbe(), newCfg.getProbe()),
                        "can't register different probes with the same name "
                            + "[name=" + name
                            + ", probe-1=" + oldCfg.getProbe()
                            + ", probe-2=" + newCfg.getProbe()
                            + "]");
                }
            } else {
                throw new IllegalArgumentException("Unsupported metric type: " + cfg);
            }
        });

        countersCfg.forEach(this::doRegister);
        probesCfg.forEach(this::doRegister);
    }

    private Metric doRegister(String name, ProbeConfig cfg) {
        assert guard.isWriteLocked() : "Thread must hold the write lock.";

        if (DEBUG) {
            log.debug("Registering probe [config={}]", cfg);
        }

        PROBE_CHECK.unique(name, allMetrics.keySet(), "name");

        DefaultProbeMetric metricProbe = new DefaultProbeMetric(name, cfg.getProbe(), cfg.getInitValue());

        probes.put(name, metricProbe);

        allMetrics.put(name, metricProbe);

        return metricProbe;
    }

    private CounterMetric doRegister(String name, CounterConfig cfg) {
        assert guard.isWriteLocked() : "Thread must hold the write lock.";

        if (DEBUG) {
            log.debug("Registering counter [config={}]", cfg);
        }

        COUNTER_CHECK.unique(name, allMetrics.keySet(), "metric name");

        CounterMetric total = null;

        String totalName = cfg.getTotalName() != null ? cfg.getTotalName().trim() : null;

        if (totalName != null) {
            COUNTER_CHECK.unique(totalName, allMetrics.keySet(), "metric name");

            total = new DefaultCounterMetric(totalName, false);

            allMetrics.put(totalName, total);
        }

        DefaultCounterMetric counter = new DefaultCounterMetric(name, cfg.isAutoReset(), total);

        counters.put(name, counter);

        allMetrics.put(name, counter);

        return counter;
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

                counters.values().forEach(cnt -> {
                    long val;

                    if (cnt.isAutoReset()) {
                        val = cnt.reset();
                    } else {
                        val = cnt.value();
                    }

                    if (snapshot != null) {
                        snapshot.put(cnt.name(), new StaticMetric(cnt.name(), val));
                    }
                });
            } else {
                snapshot = null;
            }
        } finally {
            guard.unlockWrite();
        }

        if (snapshot != null) {
            int tick = tickSeq.getAndIncrement();

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

    private String checkCounterConfig(CounterConfig cfg) {
        COUNTER_CHECK.notNull(cfg, "configuration");
        COUNTER_CHECK.notEmpty(cfg.getName(), "name");

        return cfg.getName().trim();
    }

    private String checkProbeConfig(ProbeConfig cfg) {
        PROBE_CHECK.notNull(cfg, "configuration");
        PROBE_CHECK.notEmpty(cfg.getName(), "name");
        PROBE_CHECK.notNull(cfg.getProbe(), "probe");

        return cfg.getName().trim();
    }

    @Override
    public String toString() {
        return ToString.format(LocalMetricsService.class, this);
    }
}
