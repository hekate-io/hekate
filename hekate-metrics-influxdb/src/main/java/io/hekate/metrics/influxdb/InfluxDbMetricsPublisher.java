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

package io.hekate.metrics.influxdb;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import okhttp3.OkHttpClient;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InfluxDbMetricsPublisher {
    private static class QueueEntry {
        private final long timestamp;

        private final Collection<Metric> metrics;

        private final CompletableFuture<Void> future;

        public QueueEntry(long timestamp, Collection<Metric> metrics, CompletableFuture<Void> future) {
            this.timestamp = timestamp;
            this.metrics = metrics;
            this.future = future;
        }

        public long timestamp() {
            return timestamp;
        }

        public Collection<Metric> metrics() {
            return metrics;
        }

        public CompletableFuture<Void> future() {
            return future;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(InfluxDbMetricsPlugin.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final QueueEntry STOP_ENTRY = new QueueEntry(0, Collections.emptyList(), null);

    private static final Pattern UNSAFE_CHARACTERS = Pattern.compile("[^a-z0-9.]", Pattern.CASE_INSENSITIVE);

    private final String url;

    private final String database;

    private final String user;

    @ToStringIgnore
    private final String password;

    private final int maxQueueSize;

    private final long timeout;

    private final MetricFilter filter;

    @ToStringIgnore
    private final BlockingQueue<QueueEntry> queue;

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private ExecutorService worker;

    public InfluxDbMetricsPublisher(InfluxDbMetricsConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        ConfigCheck check = ConfigCheck.get(InfluxDbMetricsConfig.class);

        check.notEmpty(cfg.getUrl(), "URL");
        check.notEmpty(cfg.getDatabase(), "database");
        check.positive(cfg.getMaxQueueSize(), "maximum queue size");
        check.positive(cfg.getTimeout(), "timeout");

        url = cfg.getUrl().trim();
        database = cfg.getDatabase().trim();
        user = cfg.getUser() != null ? cfg.getUser().trim() : null;
        password = cfg.getPassword() != null ? cfg.getPassword().trim() : null;
        maxQueueSize = cfg.getMaxQueueSize();
        timeout = cfg.getTimeout();
        filter = cfg.getFilter();

        queue = new ArrayBlockingQueue<>(maxQueueSize);
    }

    public void start(String nodeName, String nodeHost, int nodePort) {
        assert nodeHost != null : "Node host address us null.";

        log.info("Starting InfluxDB metrics publisher [{}]", ToString.formatProperties(this));

        String address = nodeHost + ':' + nodePort;

        String node;

        if (nodeName == null || nodeName.isEmpty()) {
            node = address;
        } else {
            node = nodeName;
        }

        OkHttpClient.Builder http = new OkHttpClient.Builder()
            .connectTimeout(timeout, TimeUnit.MILLISECONDS)
            .readTimeout(timeout, TimeUnit.MILLISECONDS)
            .writeTimeout(timeout, TimeUnit.MILLISECONDS);

        // Not really a connect (InfluxDB client simply initializes its internal in-memory structures).
        InfluxDB db = InfluxDBFactory.connect(url, user, password, http);

        synchronized (mux) {
            worker = Executors.newSingleThreadExecutor(new HekateThreadFactory("InfluxDbMetrics"));

            worker.execute(() -> {
                QueueEntry entry = null;
                Throwable error = null;

                try {
                    boolean throttleErrors = false;

                    while (true) {
                        entry = queue.poll(Long.MAX_VALUE, TimeUnit.MILLISECONDS);

                        if (entry == STOP_ENTRY) {
                            break;
                        }

                        boolean ok = doPublish(db, node, address, entry, throttleErrors);

                        if (ok && throttleErrors) {
                            if (log.isWarnEnabled()) {
                                log.warn("Resumed InfluxDB metrics publishing after failure.");
                            }
                        }

                        throttleErrors = !ok;
                    }
                } catch (InterruptedException e) {
                    // No-op.
                } catch (RuntimeException | Error e) {
                    error = e;

                    log.error("Got an unexpected runtime error while publishing metrics to InfluxDB.", e);

                    synchronized (mux) {
                        if (worker != null) {
                            worker.shutdown();

                            worker = null;
                        }
                    }
                } finally {
                    // Try to notify last entry.
                    try {
                        if (entry != null && entry.future() != null) {
                            if (error == null) {
                                entry.future().cancel(false);
                            } else {
                                entry.future().completeExceptionally(error);
                            }
                        }
                    } catch (Throwable t) {
                        log.error("Got an unexpected error while notifying InfluxDB publisher future.", t);
                    } finally {
                        log.info("Stopped InfluxDB metrics publisher.");

                        db.close();
                    }
                }
            });
        }
    }

    public void stop() {
        stopAsync().awaitUninterruptedly();
    }

    public boolean isStopped() {
        synchronized (mux) {
            return worker == null;
        }
    }

    public CompletableFuture<Void> publish(Collection<Metric> metrics) {
        CompletableFuture<Void> future = new CompletableFuture<>();

        boolean scheduled = false;

        if (metrics != null && !metrics.isEmpty()) {
            synchronized (mux) {
                if (worker == null) {
                    if (DEBUG) {
                        log.debug("Skipped asynchronous metrics publishing since publisher is stopped.");
                    }
                } else {
                    QueueEntry entry = new QueueEntry(System.currentTimeMillis(), metrics, future);

                    if (queue.offer(entry)) {
                        scheduled = true;

                        if (DEBUG) {
                            log.debug("Scheduled asynchronous metrics publishing [metrics-size={}]", metrics.size());
                        }
                    } else {
                        if (DEBUG) {
                            log.debug("Skipped asynchronous metrics publishing due to queue overflow [max-queue-size={}]", maxQueueSize);
                        }
                    }
                }
            }
        }

        if (!scheduled) {
            future.cancel(false);
        }

        return future;
    }

    // Package level for testing purposes.
    static String toSafeName(String str) {
        return UNSAFE_CHARACTERS.matcher(str.trim()).replaceAll("_");
    }

    // Package level for testing purposes.
    Waiting stopAsync() {
        Waiting termination = Waiting.NO_WAIT;

        List<QueueEntry> staleEntries = null;

        try {
            synchronized (mux) {
                if (worker != null) {
                    log.info("Stopping InfluxDB metrics publisher...");

                    do {
                        if (staleEntries == null) {
                            staleEntries = new ArrayList<>(queue.size());
                        }

                        queue.drainTo(staleEntries);
                    } while (!queue.offer(STOP_ENTRY));

                    termination = AsyncUtils.shutdown(worker);

                    worker = null;
                }
            }
        } finally {
            if (staleEntries != null) {
                staleEntries.stream()
                    .map(QueueEntry::future)
                    .filter(Objects::nonNull)
                    .forEach(future ->
                        future.cancel(false)
                    );
            }
        }

        return termination;
    }

    // Package level for testing purposes.
    int queueSize() {
        return queue.size();
    }

    // Package level for testing purposes.
    void doWrite(InfluxDB db, BatchPoints points) {
        db.write(points);
    }

    private boolean doPublish(InfluxDB db, String node, String address, QueueEntry entry, boolean throttle) {
        assert db != null : "Database is null.";
        assert node != null : "Node name is null.";
        assert address != null : "Node address is null.";
        assert entry != null : "Metrics queue entry is null.";

        if (DEBUG) {
            log.debug("Publishing metrics [timestamp={}, metrics-size={}]", entry.timestamp(), entry.metrics().size());
        }

        CompletableFuture<Void> future = entry.future();

        try {
            BatchPoints points = BatchPoints.database(database)
                .tag(InfluxDbMetricsPlugin.NODE_NAME_TAG, node)
                .tag(InfluxDbMetricsPlugin.NODE_ADDRESS_TAG, address)
                .build();

            entry.metrics().stream()
                .filter(metric -> filter == null || filter.accept(metric))
                .forEach(metric -> {
                    String name = toSafeName(metric.name());

                    Point point = Point.measurement(name)
                        .time(entry.timestamp(), TimeUnit.MILLISECONDS)
                        .addField(InfluxDbMetricsPlugin.METRIC_VALUE_FIELD, metric.value())
                        .build();

                    points.point(point);
                });

            if (points.getPoints() != null && !points.getPoints().isEmpty()) {
                doWrite(db, points);
            }

            if (future != null) {
                future.complete(null);
            }

            if (DEBUG) {
                log.debug("Published metrics [timestamp={}, metrics-size={}]", entry.timestamp(), entry.metrics().size());
            }

            return true;
        } catch (RuntimeException e) {
            // InfluxDB client rethrows errors as new RuntimeException(e) :(
            if (throttle) {
                if (DEBUG) {
                    log.debug("Throttled error during metrics publishing.", e);
                }
            } else {
                if (log.isWarnEnabled()) {
                    log.warn("Got an error while publishing metrics to InfluxDB (will silently ignore subsequent errors).", e);
                }
            }

            if (future != null) {
                future.completeExceptionally(e);
            }

            return false;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
