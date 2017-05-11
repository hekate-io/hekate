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

package io.hekate.javadoc.metrics.local;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.metrics.Metric;
import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.metrics.local.ProbeConfig;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LocalMetricsServiceJavadocTest extends HekateNodeTestBase {
    // Start:counter_example
    public static class ExampleService {
        private final CounterMetric active;

        public ExampleService(Hekate hekate) {
            // Register counter.
            active = hekate.localMetrics().register(new CounterConfig()
                .withName("tasks.active")
                .withTotalName("tasks.total")
            );
        }

        public void processTask(Runnable task) {
            // Increment before starting the task.
            active.increment();

            try {
                // Run some long and heavy task.
                task.run();
            } finally {
                // Decrement once tasks is completed.
                active.decrement();
            }
        }
    }
    // End:counter_example

    @Test
    public void exampleBootstrap() throws Exception {
        // Start:configure
        // Prepare service factory.
        LocalMetricsServiceFactory factory = new LocalMetricsServiceFactory()
            // Set metrics refresh interval to 1 second.
            .withRefreshInterval(1000)
            // Register some custom counter.
            .withMetric(new CounterConfig("example.counter"))
            // Register some custom probe.
            .withMetric(new ProbeConfig()
                .withName("cpu.count")
                .withProbe(() -> Runtime.getRuntime().availableProcessors())
            );

        // Start node.
        Hekate hekate = new HekateBootstrap()
            .withService(factory)
            .join();
        // End:configure

        // Start:access
        LocalMetricsService metrics = hekate.localMetrics();
        // End:access

        ExampleService exampleService = new ExampleService(hekate);

        for (int i = 0; i < 5; i++) {
            exampleService.processTask(() -> {
                // Start:counter_example_usage
                System.out.println("Active tasks: " + hekate.localMetrics().get("tasks.active"));
                System.out.println(" Total tasks: " + hekate.localMetrics().get("tasks.total"));
                // End:counter_example_usage
            });
        }

        // Start:probe_example
        // Register probe that will provide the amount of free memory in the JVM.
        hekate.localMetrics().register(new ProbeConfig("memory.free")
            .withProbe(() -> Runtime.getRuntime().freeMemory())
        );
        // End:probe_example

        Metric memoryFree = metrics.metric("memory.free");
        Metric tasksActive = metrics.metric("tasks.active");
        Metric tasksTotal = metrics.metric("tasks.total");

        assertNotNull(memoryFree);
        assertNotNull(tasksActive);
        assertNotNull(tasksTotal);

        // Start:listener
        hekate.localMetrics().addListener(event -> {
            System.out.println("Free memory: " + event.get("memory.free"));
            System.out.println("Active tasks: " + event.get("tasks.active"));
            System.out.println("Total tasks: " + event.get("tasks.total"));
        });
        // End:listener

        hekate.leave();
    }
}
