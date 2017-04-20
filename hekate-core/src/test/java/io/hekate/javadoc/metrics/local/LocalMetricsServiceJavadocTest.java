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
        private final CounterMetric activeTasks;

        private final CounterMetric totalTasks;

        public ExampleService(LocalMetricsService metrics) {
            // Register counters.
            activeTasks = metrics.register(new CounterConfig("tasks.active"));
            totalTasks = metrics.register(new CounterConfig("tasks.total"));
        }

        public void processTask(Runnable task) {
            // Increment before starting the task.
            activeTasks.increment();

            try {
                // Run some long and heavy task.
                task.run();
            } finally {
                // Decrement once tasks is finished.
                activeTasks.decrement();
            }

            // Update the total amount of processed tasks.
            totalTasks.increment();
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

        ExampleService exampleService = new ExampleService(metrics);

        exampleService.processTask(() -> {
            // No-op.
        });

        // Start:probe_example
        // Register probe that will provide the amount of free memory in the JVM.
        metrics.register(new ProbeConfig("memory.free")
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
        metrics.addListener(event -> {
            System.out.println("Free memory: " + event.get("memory.free"));
            System.out.println("Active tasks: " + event.get("tasks.active"));
            System.out.println("Total tasks: " + event.get("tasks.total"));
        });
        // End:listener

        hekate.leave();
    }
}
