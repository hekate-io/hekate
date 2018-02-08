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

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import io.hekate.metrics.MetricJmx;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LocalMetricsServiceJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        AtomicLong preProbe = new AtomicLong();

        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(LocalMetricsServiceFactory.class, metrics -> {
                metrics.withMetric(new CounterConfig()
                    .withName("pre-c")
                    .withTotalName("pre-c.total")
                    .withAutoReset(true)
                );
                metrics.withMetric(new ProbeConfig()
                    .withName("pre-p")
                    .withProbe(preProbe::get)
                );
                metrics.withMetric(new TimerConfig()
                    .withName("pre-t")
                    .withRateName("pre-t.rate")
                );
            });
        }).join();

        JmxService jmx = node.get(JmxService.class);

        ObjectName metricsName = jmx.nameFor(LocalMetricsServiceJmx.class);

        repeat(3, i -> {
            if (node.state() == Hekate.State.DOWN) {
                node.join();
            }

            AtomicLong probe = new AtomicLong();

            node.localMetrics().register(new CounterConfig()
                .withName("c")
                .withTotalName("c.total")
                .withAutoReset(true)
            );

            node.localMetrics().register(new ProbeConfig()
                .withName("p")
                .withProbe(probe::get)
            );

            node.localMetrics().register(new TimerConfig()
                .withName("t")
                .withRateName("t.rate")
            );

            CounterMetric preCounter = node.localMetrics().counter("pre-c");
            CounterMetric counter = node.localMetrics().counter("c");

            assertEquals(node.localMetrics().refreshInterval(), ((Long)jmxAttribute(metricsName, "RefreshInterval", node)).longValue());
            assertNotNull(jmxAttribute(metricsName, "MetricsSnapshot", node));

            ObjectName jmxPreCounter = jmx.nameFor(MetricJmx.class, "pre-c");
            ObjectName jmxPreCounterTot = jmx.nameFor(MetricJmx.class, "pre-c.total");
            ObjectName jmxPreProbe = jmx.nameFor(MetricJmx.class, "pre-p");
            ObjectName jmxPreTimer = jmx.nameFor(MetricJmx.class, "pre-t");
            ObjectName jmxPreTimerRate = jmx.nameFor(MetricJmx.class, "pre-t.rate");

            ObjectName jmxCounter = jmx.nameFor(MetricJmx.class, "c");
            ObjectName jmxCounterTot = jmx.nameFor(MetricJmx.class, "c.total");
            ObjectName jmxProbe = jmx.nameFor(MetricJmx.class, "p");

            assertEquals("pre-c", jmxAttribute(jmxPreCounter, "Name", node));
            assertEquals("pre-c.total", jmxAttribute(jmxPreCounterTot, "Name", node));
            assertEquals("pre-p", jmxAttribute(jmxPreProbe, "Name", node));
            assertEquals("pre-t", jmxAttribute(jmxPreTimer, "Name", node));
            assertEquals("pre-t.rate", jmxAttribute(jmxPreTimerRate, "Name", node));

            preCounter.increment();
            preProbe.incrementAndGet();

            counter.increment();
            probe.incrementAndGet();

            busyWait("pre-counter.total", () -> jmxAttribute(jmxPreCounterTot, "Value", node).equals(1L));
            busyWait("pre-probe", () -> jmxAttribute(jmxPreProbe, "Value", node).equals((long)(i + 1)));

            busyWait("counter.total", () -> jmxAttribute(jmxCounterTot, "Value", node).equals(1L));
            busyWait("probe", () -> jmxAttribute(jmxProbe, "Value", node).equals(1L));

            busyWait("pre-counter", () -> jmxAttribute(jmxPreCounter, "Value", node).equals(1L));
            busyWait("counter", () -> jmxAttribute(jmxCounter, "Value", node).equals(1L));

            node.leave();
        });
    }
}
