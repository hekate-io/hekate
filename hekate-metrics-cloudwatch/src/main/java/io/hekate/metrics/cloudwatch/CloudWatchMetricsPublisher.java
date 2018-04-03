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

package io.hekate.metrics.cloudwatch;

import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toList;

class CloudWatchMetricsPublisher {
    interface CloudWatchClient {
        void putMetrics(PutMetricDataRequest request);
    }

    public static final int MAX_METRICS_PER_REQUEST = 20;

    private static final Logger log = LoggerFactory.getLogger(CloudWatchMetricsPlugin.class);

    private final long interval;

    private final String namespace;

    private final String instanceId;

    private final MetricFilter filter;

    @ToStringIgnore
    private final CloudWatchClient cloudWatch;

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private final Map<String, StatisticSet> stats = new HashMap<>();

    @ToStringIgnore
    private ScheduledExecutorService worker;

    @ToStringIgnore
    private volatile boolean throttleAsyncErrors;

    public CloudWatchMetricsPublisher(
        long interval,
        String namespace,
        String instanceId,
        MetricFilter filter,
        CloudWatchClient cloudWatch
    ) {
        assert cloudWatch != null : "CloudWatch client is null.";
        assert interval > 0 : "Interval must be above zero [value=" + interval + ']';
        assert namespace != null : "Metrics namespace is null.";
        assert instanceId != null : "AWS instance ID is null.";

        this.interval = interval;
        this.namespace = namespace;
        this.instanceId = instanceId;
        this.filter = filter;
        this.cloudWatch = cloudWatch;
    }

    public void publish(Collection<Metric> metrics) {
        assert metrics != null : "Metrics are null.";

        synchronized (mux) {
            if (isStarted()) {
                Collection<Metric> filteredMetrics;

                // Check if we need to filter metrics.
                if (filter == null) {
                    // No filtering.
                    filteredMetrics = metrics;
                } else {
                    // Apply filter.
                    filteredMetrics = metrics.stream()
                        .filter(filter::accept)
                        .collect(toList());
                }

                // Aggregate metrics as CloudWatch statistic sets.
                filteredMetrics.forEach(m -> {
                    StatisticSet stat = stats.computeIfAbsent(m.name(), name -> new StatisticSet());

                    double val = m.value();

                    if (stat.getSum() == null) {
                        stat.setSum(val);
                    } else {
                        stat.setSum(val + stat.getSum());
                    }

                    if (stat.getMaximum() == null) {
                        stat.setMaximum(val);
                    } else {
                        stat.setMaximum(Math.max(val, stat.getMaximum()));
                    }

                    if (stat.getMinimum() == null) {
                        stat.setMinimum(val);
                    } else {
                        stat.setMinimum(Math.min(val, stat.getMinimum()));
                    }

                    if (stat.getSampleCount() == null) {
                        stat.setSampleCount(1d);
                    } else {
                        stat.setSampleCount(stat.getSampleCount() + 1);
                    }
                });
            }
        }
    }

    public void start(String nodeName) {
        log.info("Starting CloudWatch metrics publisher [{}]", ToString.formatProperties(this));

        synchronized (mux) {
            worker = Executors.newSingleThreadScheduledExecutor(new HekateThreadFactory("CloudWatchMetrics"));

            worker.scheduleWithFixedDelay(() -> {
                try {
                    if (submitToCloudWatch(nodeName)) {
                        if (throttleAsyncErrors) {
                            log.info("Recovered metrics publishing to AWS CloudWatch.");
                        }

                        throttleAsyncErrors = false;
                    }
                } catch (Throwable t) {
                    if (!throttleAsyncErrors) {
                        throttleAsyncErrors = true;

                        log.error("Failed to publish metrics to AWS CloudWatch (will throttle subsequent errors until recovered).", t);
                    }
                }
            }, interval, interval, TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        Waiting shutdown = null;

        synchronized (mux) {
            if (worker != null) {
                log.info("Stopping CloudWatch metrics publisher...");

                shutdown = AsyncUtils.shutdown(worker);

                worker = null;
            }

            stats.clear();
        }

        if (shutdown != null) {
            shutdown.awaitUninterruptedly();

            log.info("Stopped CloudWatch metrics publisher.");
        }
    }

    // Package level for testing purposes.
    boolean submitToCloudWatch(String fromNode) {
        assert fromNode != null : "Node name is null.";

        List<PutMetricDataRequest> requests = null;

        synchronized (mux) {
            if (isStarted() && !stats.isEmpty()) {
                requests = new ArrayList<>();

                Date now = new Date();

                Dimension[] dimensions = newDimensions(fromNode);

                PutMetricDataRequest request = newRequest();

                int idx = 0;

                // Split metrics up to MAX_METRICS_PER_REQUEST per request.
                for (Map.Entry<String, StatisticSet> e : stats.entrySet()) {
                    if (idx > 0 && idx % MAX_METRICS_PER_REQUEST == 0) {
                        requests.add(request);

                        request = newRequest();
                    }

                    String name = Utils.camelCase(e.getKey());

                    StatisticSet value = e.getValue();

                    request.withMetricData(new MetricDatum()
                        .withUnit(StandardUnit.None)
                        .withTimestamp(now)
                        .withDimensions(dimensions)
                        .withMetricName(name)
                        .withStatisticValues(value)
                    );

                    idx++;
                }

                requests.add(request);

                stats.clear();
            }
        }

        if (requests != null && !requests.isEmpty()) {
            // Submit to CloudWatch.
            requests.forEach(cloudWatch::putMetrics);

            return true;
        } else {
            return false;
        }
    }

    // Package level for testing purposes.
    boolean isStarted() {
        synchronized (mux) {
            return worker != null;
        }
    }

    // Package level for testing purposes.
    int aggregatedStatsCount() {
        synchronized (mux) {
            return stats.size();
        }
    }

    // Package level for testing purposes.
    boolean isThrottleAsyncErrors() {
        return throttleAsyncErrors;
    }

    private PutMetricDataRequest newRequest() {
        return new PutMetricDataRequest()
            .withNamespace(namespace);
    }

    private Dimension[] newDimensions(String fromNode) {
        assert fromNode != null : "Node name is null.";

        List<Dimension> dimensions = new ArrayList<>();

        dimensions.add(new Dimension().withName("InstanceId").withValue(instanceId));

        if (!fromNode.isEmpty()) {
            dimensions.add(new Dimension().withName("NodeName").withValue(fromNode));
        }

        return dimensions.toArray(new Dimension[dimensions.size()]);
    }
}
