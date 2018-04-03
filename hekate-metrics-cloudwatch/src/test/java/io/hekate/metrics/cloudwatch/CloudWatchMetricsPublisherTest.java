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
import io.hekate.HekateTestBase;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricValue;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CloudWatchMetricsPublisherTest extends HekateTestBase {
    private static final String TEST_NODE_NAME = "TestNode";

    private final CloudWatchClientRequestCaptor captor = new CloudWatchClientRequestCaptor();

    private final CloudWatchMetricsPublisher publisher = new CloudWatchMetricsPublisher(
        Long.MAX_VALUE,
        "TestNamespace",
        "TestInstanceId",
        null,
        captor
    );

    @Before
    public void setUp() {
        publisher.start(TEST_NODE_NAME);
    }

    @After
    public void tearDown() {
        publisher.stop();
    }

    @Test
    public void testStartStop() throws Exception {
        repeat(3, i -> {
            assertTrue(publisher.isStarted());

            publisher.publish(Collections.singleton(newMetric("test", 100500)));

            assertEquals(1, publisher.aggregatedStatsCount());

            publisher.stop();

            assertFalse(publisher.isStarted());

            assertEquals(0, publisher.aggregatedStatsCount());

            publisher.publish(Collections.singleton(newMetric("test", 100500)));

            assertEquals(0, publisher.aggregatedStatsCount());

            publisher.start(TEST_NODE_NAME);
        });
    }

    @Test
    public void testPublishEmptyMetrics() {
        publisher.publish(Collections.emptyList());
        publisher.submitToCloudWatch("ignore");

        assertTrue(captor.requests().isEmpty());
    }

    @Test
    public void testPublishSingleMetric() {
        publisher.publish(Collections.singleton(newMetric("test", 100500)));
        publisher.submitToCloudWatch(TEST_NODE_NAME);

        assertEquals(1, captor.requests().size());

        PutMetricDataRequest req = captor.requests().get(0);

        verifyCommonRequestData(req);

        StatisticSet stats = extractStats("Test", req);

        assertEquals(1, stats.getSampleCount().intValue());
        assertEquals(100500, stats.getSum().intValue());
        assertEquals(100500, stats.getMaximum().intValue());
        assertEquals(100500, stats.getMinimum().intValue());
    }

    @Test
    public void testPublishLessThanMaxPerRequest() {
        for (int i = 1; i <= 3; i++) {
            List<Metric> metrics = new ArrayList<>();

            for (int j = 0; j < CloudWatchMetricsPublisher.MAX_METRICS_PER_REQUEST - 1; j++) {
                metrics.add(newMetric("test" + j, i));
            }

            publisher.publish(metrics);
        }

        publisher.submitToCloudWatch(TEST_NODE_NAME);

        assertEquals(1, captor.requests().size());

        PutMetricDataRequest req = captor.requests().get(0);

        verifyCommonRequestData(req);

        for (int i = 0; i < CloudWatchMetricsPublisher.MAX_METRICS_PER_REQUEST - 1; i++) {
            StatisticSet stats = extractStats("Test" + i, req);

            assertEquals(3, stats.getSampleCount().intValue());
            assertEquals(6, stats.getSum().intValue());
            assertEquals(1, stats.getMinimum().intValue());
            assertEquals(3, stats.getMaximum().intValue());
        }
    }

    @Test
    public void testPublishSameAsMaxPerRequest() {
        for (int i = 1; i <= 3; i++) {
            List<Metric> metrics = new ArrayList<>();

            for (int j = 0; j < CloudWatchMetricsPublisher.MAX_METRICS_PER_REQUEST; j++) {
                metrics.add(newMetric("test" + j, i));
            }

            publisher.publish(metrics);
        }

        publisher.submitToCloudWatch(TEST_NODE_NAME);

        assertEquals(1, captor.requests().size());

        PutMetricDataRequest req = captor.requests().get(0);

        verifyCommonRequestData(req);

        for (int i = 0; i < CloudWatchMetricsPublisher.MAX_METRICS_PER_REQUEST; i++) {
            StatisticSet stats = extractStats("Test" + i, req);

            assertEquals(3, stats.getSampleCount().intValue());
            assertEquals(6, stats.getSum().intValue());
            assertEquals(1, stats.getMinimum().intValue());
            assertEquals(3, stats.getMaximum().intValue());
        }
    }

    @Test
    public void testPublishGreaterThanMaxPerRequest() {
        int maxMetricsPerRequest = CloudWatchMetricsPublisher.MAX_METRICS_PER_REQUEST;

        for (int i = 1; i <= 3; i++) {
            List<Metric> metrics = new ArrayList<>();

            for (int j = 0; j < maxMetricsPerRequest * 3; j++) {
                metrics.add(newMetric("test" + j, i));
            }

            publisher.publish(metrics);
        }

        publisher.submitToCloudWatch(TEST_NODE_NAME);

        assertEquals(3, captor.requests().size());

        Map<String, StatisticSet> allStats = new HashMap<>();

        captor.requests().forEach(req -> {
            verifyCommonRequestData(req);

            assertEquals(maxMetricsPerRequest, req.getMetricData().size());

            req.getMetricData().forEach(d ->
                allStats.put(d.getMetricName(), d.getStatisticValues())
            );
        });

        for (int i = 0; i < maxMetricsPerRequest * 3; i++) {
            StatisticSet stats = allStats.get("Test" + i);

            assertEquals(3, stats.getSampleCount().intValue());
            assertEquals(6, stats.getSum().intValue());
            assertEquals(1, stats.getMinimum().intValue());
            assertEquals(3, stats.getMaximum().intValue());
        }
    }

    @Test
    public void testSchedule() throws Exception {
        CloudWatchMetricsPublisher publisher = new CloudWatchMetricsPublisher(
            10,
            "TestNamespace",
            "TestInstanceId",
            null,
            captor
        );

        publisher.start(TEST_NODE_NAME);

        try {
            repeat(3, i -> {
                publisher.publish(Collections.singleton(newMetric("test", 100500)));

                busyWait("metrics published", () -> !captor.requests().isEmpty());

                verifyCommonRequestData(captor.requests().get(0));

                captor.requests().clear();
            });

        } finally {
            publisher.stop();
        }
    }

    @Test
    public void testFilter() throws Exception {
        CloudWatchMetricsPublisher publisher = new CloudWatchMetricsPublisher(
            Long.MAX_VALUE,
            "TestNamespace",
            "TestInstanceId",
            m -> m.name().startsWith("include"),
            captor
        );

        publisher.start(TEST_NODE_NAME);

        try {
            repeat(3, i -> {
                publisher.publish(Arrays.asList(
                    newMetric("exclude1", 100500),
                    newMetric("exclude2", 100500),
                    newMetric("include1", 100500),
                    newMetric("include2", 100500)
                ));

                publisher.submitToCloudWatch(TEST_NODE_NAME);

                busyWait("metrics published", () -> !captor.requests().isEmpty());

                assertEquals(1, captor.requests().size());

                PutMetricDataRequest req = captor.requests().get(0);

                verifyCommonRequestData(req);

                assertEquals(2, req.getMetricData().size());
                assertTrue(req.getMetricData().stream().allMatch(d -> d.getMetricName().startsWith("Include")));

                captor.requests().clear();
            });

        } finally {
            publisher.stop();
        }
    }

    @Test
    public void testPublishingError() throws Exception {
        AtomicBoolean fail = new AtomicBoolean();

        CloudWatchMetricsPublisher publisher = new CloudWatchMetricsPublisher(
            10,
            "TestNamespace",
            "TestInstanceId",
            null,
            request -> {
                if (fail.get()) {
                    throw TEST_ERROR;
                } else {
                    captor.putMetrics(request);
                }
            }
        );

        publisher.start(TEST_NODE_NAME);

        try {
            repeat(3, i -> {
                fail.set(true);

                publisher.publish(Collections.singleton(newMetric("test", 100500)));

                busyWait("errors throttling enabled", publisher::isThrottleAsyncErrors);

                fail.set(false);

                publisher.publish(Collections.singleton(newMetric("test", 100500)));

                busyWait("metrics published", () -> !captor.requests().isEmpty());

                assertFalse(publisher.isThrottleAsyncErrors());

                verifyCommonRequestData(captor.requests().get(0));

                captor.requests().clear();
            });

        } finally {
            publisher.stop();
        }
    }

    private StatisticSet extractStats(String metric, PutMetricDataRequest req) {
        return req.getMetricData().stream()
            .filter(it -> metric.equals(it.getMetricName()))
            .map(MetricDatum::getStatisticValues)
            .findFirst()
            .orElseThrow(() -> new AssertionError(metric));
    }

    private void verifyCommonRequestData(PutMetricDataRequest req) {
        assertEquals("TestNamespace", req.getNamespace());

        List<MetricDatum> data = req.getMetricData();

        if (data != null) {
            data.forEach(d -> {
                assertNotNull(d.getTimestamp());
                assertEquals(StandardUnit.None.toString(), d.getUnit());
                assertNull(d.getStorageResolution());

                assertEquals("TestInstanceId", extractDimensionValue("InstanceId", d));
                assertEquals(TEST_NODE_NAME, extractDimensionValue("NodeName", d));

                StatisticSet stats = d.getStatisticValues();

                assertNotNull(stats);
                assertNotNull(stats.getMaximum());
                assertNotNull(stats.getMinimum());
                assertNotNull(stats.getSum());
                assertNotNull(stats.getSampleCount());
            });
        }
    }

    private String extractDimensionValue(String name, MetricDatum d) {
        return d.getDimensions().stream()
            .filter(it -> name.equals(it.getName()))
            .map(Dimension::getValue)
            .findFirst()
            .orElseThrow(AssertionError::new);
    }

    private Metric newMetric(String name, long value) {
        return new MetricValue(name, value);
    }
}
