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

package io.hekate.metrics.cluster.internal;

import io.hekate.HekateInstanceContextTestBase;
import io.hekate.HekateTestContext;
import io.hekate.core.HekateTestInstance;
import io.hekate.metrics.CounterConfig;
import io.hekate.metrics.CounterMetric;
import io.hekate.metrics.MetricsService;
import io.hekate.metrics.MetricsServiceFactory;
import io.hekate.metrics.ProbeConfig;
import io.hekate.metrics.cluster.ClusterMetricsService;
import io.hekate.metrics.cluster.ClusterMetricsServiceFactory;
import io.hekate.metrics.cluster.ClusterNodeMetrics;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class ClusterMetricsServiceTest extends HekateInstanceContextTestBase {
    public interface ClusterMetricsConfigurer {
        void configure(ClusterMetricsServiceFactory factory);
    }

    public static final int TEST_METRICS_REFRESH_INTERVAL = 25;

    public ClusterMetricsServiceTest(HekateTestContext params) {
        super(params);
    }

    @Test
    public void testAddMetrics() throws Exception {
        HekateTestInstance inst1 = createMetricsInstance();
        HekateTestInstance inst2 = createMetricsInstance();
        HekateTestInstance inst3 = createMetricsInstance();

        MetricsService s1 = inst1.join().get(MetricsService.class);
        MetricsService s2 = inst2.join().get(MetricsService.class);
        MetricsService s3 = inst3.join().get(MetricsService.class);

        awaitForTopology(inst1, inst2, inst3);

        repeat(5, i -> {
            CounterMetric c1 = s1.register(new CounterConfig("c1_" + i));
            CounterMetric c2 = s2.register(new CounterConfig("c2_" + i));
            CounterMetric c3 = s3.register(new CounterConfig("c3_" + i));

            awaitForReplicatedMetric("c1_" + i, 0, inst1, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c2_" + i, 0, inst2, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c3_" + i, 0, inst3, Arrays.asList(inst1, inst2, inst3));

            c1.add(100);
            c2.add(200);
            c3.add(300);

            awaitForReplicatedMetric("c1_" + i, 100, inst1, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c2_" + i, 200, inst2, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c3_" + i, 300, inst3, Arrays.asList(inst1, inst2, inst3));

            c1.add(1000);
            c2.add(2000);
            c3.add(3000);

            awaitForReplicatedMetric("c1_" + i, 1100, inst1, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c2_" + i, 2200, inst2, Arrays.asList(inst1, inst2, inst3));
            awaitForReplicatedMetric("c3_" + i, 3300, inst3, Arrays.asList(inst1, inst2, inst3));
        });
    }

    @Test
    public void testMetricsWithNoMetricsNode() throws Exception {
        HekateTestInstance inst1 = createMetricsInstance();
        HekateTestInstance inst2 = createMetricsInstance();

        HekateTestInstance noMetricsNode = createInstance();

        MetricsService s1 = inst1.join().get(MetricsService.class);
        MetricsService s2 = inst2.join().get(MetricsService.class);

        noMetricsNode.join();

        awaitForTopology(inst1, inst2, noMetricsNode);

        repeat(5, i -> {
            CounterMetric c1 = s1.register(new CounterConfig("c1_" + i));
            CounterMetric c2 = s2.register(new CounterConfig("c2_" + i));

            awaitForReplicatedMetric("c1_" + i, 0, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 0, inst2, Arrays.asList(inst1, inst2));

            c1.add(100);
            c2.add(200);

            awaitForReplicatedMetric("c1_" + i, 100, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 200, inst2, Arrays.asList(inst1, inst2));

            c1.add(1000);
            c2.add(2000);

            awaitForReplicatedMetric("c1_" + i, 1100, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 2200, inst2, Arrays.asList(inst1, inst2));
        });
    }

    @Test
    public void testMetricsFilter() throws Exception {
        HekateTestInstance inst1 = createMetricsInstance(f -> f.setReplicationFilter(m -> m.getName().startsWith("c")));
        HekateTestInstance inst2 = createMetricsInstance(f -> f.setReplicationFilter(m -> m.getName().startsWith("c")));

        MetricsService s1 = inst1.join().get(MetricsService.class);
        MetricsService s2 = inst2.join().get(MetricsService.class);

        ClusterMetricsService cs1 = inst1.get(ClusterMetricsService.class);
        ClusterMetricsService cs2 = inst2.get(ClusterMetricsService.class);

        awaitForTopology(inst1, inst2);

        repeat(5, i -> {
            CounterMetric no1 = s1.register(new CounterConfig("no1_" + i));
            CounterMetric no2 = s2.register(new CounterConfig("no2_" + i));

            CounterMetric c1 = s1.register(new CounterConfig("c1_" + i));
            CounterMetric c2 = s2.register(new CounterConfig("c2_" + i));

            awaitForReplicatedMetric("c1_" + i, 0, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 0, inst2, Arrays.asList(inst1, inst2));

            assertNull(cs1.forNode(inst2.getNode()).get().getMetric("no_2" + i));
            assertNull(cs2.forNode(inst1.getNode()).get().getMetric("no_1" + i));

            c1.add(100);
            c2.add(200);

            no1.add(1000);
            no2.add(1000);

            awaitForReplicatedMetric("c1_" + i, 100, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 200, inst2, Arrays.asList(inst1, inst2));

            assertNull(cs1.forNode(inst2.getNode()).get().getMetric("no_2" + i));
            assertNull(cs2.forNode(inst1.getNode()).get().getMetric("no_1" + i));

            c1.add(1000);
            c2.add(2000);

            awaitForReplicatedMetric("c1_" + i, 1100, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 2200, inst2, Arrays.asList(inst1, inst2));

            assertNull(cs1.forNode(inst2.getNode()).get().getMetric("no_2" + i));
            assertNull(cs2.forNode(inst1.getNode()).get().getMetric("no_1" + i));
        });
    }

    @Test
    public void testGetMetricsWithFilter() throws Exception {
        HekateTestInstance inst1 = createMetricsInstance();
        HekateTestInstance inst2 = createMetricsInstance();

        MetricsService s1 = inst1.join().get(MetricsService.class);
        MetricsService s2 = inst2.join().get(MetricsService.class);

        ClusterMetricsService cs1 = inst1.get(ClusterMetricsService.class);
        ClusterMetricsService cs2 = inst2.get(ClusterMetricsService.class);

        awaitForTopology(inst1, inst2);

        repeat(5, i -> {
            s1.register(new CounterConfig("no1_" + i));
            s2.register(new CounterConfig("no2_" + i));

            s1.register(new CounterConfig("c1_" + i));
            s2.register(new CounterConfig("c2_" + i));

            awaitForReplicatedMetric("c1_" + i, 0, inst1, Arrays.asList(inst1, inst2));
            awaitForReplicatedMetric("c2_" + i, 0, inst2, Arrays.asList(inst1, inst2));

            assertTrue(cs1.getMetrics(m -> m.getName().startsWith("c")).stream().anyMatch(n -> n.getNode().equals(inst1.getNode())));
            assertTrue(cs2.getMetrics(m -> m.getName().startsWith("c")).stream().anyMatch(n -> n.getNode().equals(inst1.getNode())));

            assertTrue(cs1.getMetrics(m -> m.getName().startsWith("no1")).stream().noneMatch(n -> n.getNode().equals(inst2.getNode())));
            assertTrue(cs2.getMetrics(m -> m.getName().startsWith("no2")).stream().noneMatch(n -> n.getNode().equals(inst1.getNode())));

            assertTrue(cs1.getMetrics(m -> false).isEmpty());
            assertTrue(cs2.getMetrics(m -> false).isEmpty());
        });
    }

    @Test
    public void testAddRemoveNodes() throws Exception {
        List<HekateTestInstance> instances = new LinkedList<>();
        List<MetricsService> services = new LinkedList<>();
        List<ClusterMetricsService> clusterServices = new LinkedList<>();
        List<AtomicInteger> probes = new LinkedList<>();

        sayHeader("Start instances.");

        repeat(5, i -> {
            for (MetricsService metrics : services) {
                metrics.getCounter("c").add(1);
            }

            for (AtomicInteger probe : probes) {
                probe.set(i);
            }

            HekateTestInstance instance = createMetricsInstance();

            instances.add(instance);

            instance.join();

            MetricsService metrics = instance.get(MetricsService.class);
            ClusterMetricsService clusterMetrics = instance.get(ClusterMetricsService.class);

            services.add(metrics);
            clusterServices.add(clusterMetrics);

            AtomicInteger probe = new AtomicInteger(i);

            probes.add(probe);

            metrics.register(new CounterConfig("c"));
            metrics.register(new ProbeConfig("p").withProbe(probe::get).withInitValue(i));

            metrics.getCounter("c").add(i);

            awaitForReplicatedMetric("c", i, instances);
            awaitForReplicatedMetric("p", i, instances);

            for (ClusterMetricsService service : clusterServices) {
                assertEquals(instances.size(), service.getMetrics().size());

                for (HekateTestInstance inst : instances) {
                    ClusterNodeMetrics nodeMetrics = service.forNode(inst.getNode()).get();

                    assertNotNull(nodeMetrics.getMetric("c"));
                    assertTrue(nodeMetrics.getAll().containsKey("c"));
                    assertEquals(inst.getNode(), nodeMetrics.getNode());
                    assertEquals(i, nodeMetrics.getMetric("c").getValue());

                    assertNotNull(nodeMetrics.getMetric("p"));
                    assertTrue(nodeMetrics.getAll().containsKey("p"));
                    assertEquals(inst.getNode(), nodeMetrics.getNode());
                    assertEquals(i, nodeMetrics.getMetric("p").getValue());
                }
            }
        });

        sayHeader("Update metrics.");

        repeat(5, i -> {
            long oldVal = 4;

            for (MetricsService metrics : services) {
                metrics.getCounter("c").add(100000);
            }

            for (AtomicInteger probe : probes) {
                probe.set(100000 + i);
            }

            awaitForReplicatedMetric("c", 100000 + oldVal, instances);
            awaitForReplicatedMetric("p", 100000 + i, instances);

            for (MetricsService metrics : services) {
                metrics.getCounter("c").subtract(100000);
            }

            for (AtomicInteger probe : probes) {
                probe.set(i);
            }

            awaitForReplicatedMetric("c", oldVal, instances);
            awaitForReplicatedMetric("p", i, instances);
        });

        sayHeader("Stop instances.");

        repeat(5, i -> {
            instances.remove(0).leave();
            services.remove(0);
            clusterServices.remove(0);

            awaitForTopology(instances);

            long oldVal = 4;

            for (MetricsService metrics : services) {
                metrics.getCounter("c").add(1000);
            }

            for (AtomicInteger probe : probes) {
                probe.set(1000 + i);
            }

            awaitForReplicatedMetric("c", 1000 + oldVal, instances);
            awaitForReplicatedMetric("p", 1000 + i, instances);

            for (ClusterMetricsService service : clusterServices) {
                assertEquals(instances.size(), service.getMetrics().size());

                for (HekateTestInstance inst : instances) {
                    ClusterNodeMetrics nodeMetrics = service.forNode(inst.getNode()).get();

                    assertNotNull(nodeMetrics.getMetric("c"));
                    assertTrue(nodeMetrics.getAll().containsKey("c"));
                    assertEquals(inst.getNode(), nodeMetrics.getNode());
                    assertEquals(1000 + oldVal, nodeMetrics.getMetric("c").getValue());

                    assertNotNull(nodeMetrics.getMetric("p"));
                    assertTrue(nodeMetrics.getAll().containsKey("p"));
                    assertEquals(inst.getNode(), nodeMetrics.getNode());
                    assertEquals(1000 + i, nodeMetrics.getMetric("p").getValue());
                }
            }

            for (MetricsService metrics : services) {
                metrics.getCounter("c").subtract(1000);
            }

            for (AtomicInteger probe : probes) {
                probe.set(i);
            }

            awaitForReplicatedMetric("c", oldVal, instances);
            awaitForReplicatedMetric("p", i, instances);
        });
    }

    @Test
    public void testTerminateNode() throws Exception {
        disableNodeFailurePostCheck();

        HekateTestInstance inst1 = createMetricsInstance();
        HekateTestInstance inst2 = createMetricsInstance();
        HekateTestInstance inst3 = createMetricsInstance();

        MetricsService s1 = inst1.join().get(MetricsService.class);
        MetricsService s2 = inst2.join().get(MetricsService.class);

        inst3.join().get(MetricsService.class);

        awaitForTopology(inst1, inst2, inst3);

        CounterMetric c1 = s1.register(new CounterConfig("c1"));
        CounterMetric c2 = s2.register(new CounterConfig("c2"));

        c1.add(1);
        c2.add(2);

        awaitForReplicatedMetric("c1", 1, inst1, Arrays.asList(inst1, inst2, inst3));
        awaitForReplicatedMetric("c2", 2, inst2, Arrays.asList(inst1, inst2, inst3));

        say("Terminating instance...");

        inst3.terminate();

        awaitForTopology(inst1, inst2);

        c1.add(100);
        c2.add(200);

        awaitForReplicatedMetric("c1", 101, inst1, Arrays.asList(inst1, inst2));
        awaitForReplicatedMetric("c2", 202, inst2, Arrays.asList(inst1, inst2));
    }

    protected HekateTestInstance createMetricsInstance() throws Exception {
        return createMetricsInstance(null);
    }

    protected HekateTestInstance createMetricsInstance(ClusterMetricsConfigurer configurer) throws Exception {
        return createInstance(c -> {
            MetricsServiceFactory metrics = new MetricsServiceFactory();

            metrics.setRefreshInterval(TEST_METRICS_REFRESH_INTERVAL);

            c.withService(metrics);

            ClusterMetricsServiceFactory clusterMetrics = new ClusterMetricsServiceFactory();

            clusterMetrics.setReplicationInterval(TEST_METRICS_REFRESH_INTERVAL);

            if (configurer != null) {
                configurer.configure(clusterMetrics);
            }

            c.withService(clusterMetrics);
        });
    }

    private void awaitForReplicatedMetric(String metric, long value, HekateTestInstance fromNode, List<HekateTestInstance> nodes)
        throws Exception {
        awaitForReplicatedMetric(metric, value, Collections.singletonList(fromNode), nodes);
    }

    private void awaitForReplicatedMetric(String metric, long value, List<HekateTestInstance> nodes) throws Exception {
        awaitForReplicatedMetric(metric, value, nodes, nodes);
    }

    private void awaitForReplicatedMetric(String metric, long value, List<HekateTestInstance> checkNodes, List<HekateTestInstance> nodes)
        throws Exception {
        busyWait("metric value [name=" + metric + ", value=" + value + ", nodes=" + nodes + ']', () -> {
            boolean hasValue = true;

            for (HekateTestInstance node : nodes) {
                ClusterMetricsService service = node.get(ClusterMetricsService.class);

                for (HekateTestInstance checkNode : checkNodes) {
                    ClusterNodeMetrics metrics = service.forNode(checkNode.getNode()).orElse(null);

                    if (metrics == null || metrics.getMetric(metric) == null || metrics.getMetric(metric).getValue() != value) {
                        return false;
                    }
                }
            }

            return true;
        });
    }
}
