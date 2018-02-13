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
import io.hekate.metrics.local.TimeSpan;
import io.hekate.metrics.local.TimerConfig;
import io.hekate.metrics.local.TimerMetric;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class LocalMetricsServiceJavadocTest extends HekateNodeTestBase {
    // Start:counter_example
    public static class CounterExampleService {
        private final CounterMetric tasks;

        public CounterExampleService(Hekate hekate) {
            // Register counter.
            tasks = hekate.localMetrics().register(new CounterConfig()
                .withName("task.active")
                .withTotalName("task.total")
            );
        }

        public void processTask(Runnable task) {
            // Increment before starting the task.
            tasks.increment();

            try {
                // Run some long and heavy task.
                task.run();
            } finally {
                // Decrement once tasks is completed.
                tasks.decrement();
            }
        }
    }
    // End:counter_example

    // Start:timer_example
    public static class TimerExampleService {
        private final TimerMetric timer;

        public TimerExampleService(Hekate hekate) {
            // Register timer.
            timer = hekate.localMetrics().register(new TimerConfig()
                .withName("task.time")
                .withRateName("task.rate")
                .withTimeUnit(TimeUnit.MICROSECONDS)
            );
        }

        public void processTask(Runnable task) {
            try (TimeSpan time = timer.start()) {
                // Run the task.
                task.run();
            }
        }
    }
    // End:timer_example

    @Test
    public void exampleBootstrap() throws Exception {
        // Start:configure
        // Prepare service factory.
        LocalMetricsServiceFactory factory = new LocalMetricsServiceFactory()
            // Set metrics refresh interval to 1 second.
            .withRefreshInterval(1000)
            // Register counter.
            .withMetric(new CounterConfig("example.counter"))
            // Register timer.
            .withMetric(new TimerConfig("example.timer"))
            // Register probe.
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

        CounterExampleService counterExampleService = new CounterExampleService(hekate);
        TimerExampleService timerExampleService = new TimerExampleService(hekate);

        for (int i = 0; i < 5; i++) {
            counterExampleService.processTask(() -> {
                // Start:counter_example_usage
                System.out.println("Active tasks: " + hekate.localMetrics().get("task.active"));
                System.out.println(" Total tasks: " + hekate.localMetrics().get("task.total"));
                // End:counter_example_usage
            });

            timerExampleService.processTask(() -> {
                // Start:timer_example_usage
                System.out.println("Task time: " + hekate.localMetrics().get("task.time"));
                System.out.println("Task rate: " + hekate.localMetrics().get("task.rate"));
                // End:timer_example_usage
            });
        }

        // Start:probe_example
        // Register probe that will provide the amount of free memory in the JVM.
        hekate.localMetrics().register(new ProbeConfig("memory.free")
            .withProbe(() -> Runtime.getRuntime().freeMemory())
        );
        // End:probe_example

        Metric memoryFree = metrics.metric("memory.free");
        Metric taskActive = metrics.metric("task.active");
        Metric taskTotal = metrics.metric("task.total");
        Metric taskTime = metrics.metric("task.time");
        Metric taskRate = metrics.metric("task.rate");

        assertNotNull(memoryFree);
        assertNotNull(taskActive);
        assertNotNull(taskTotal);
        assertNotNull(taskTime);
        assertNotNull(taskRate);

        // Start:listener
        // Listen for metrics updates.
        hekate.localMetrics().addListener(event -> {
            System.out.println("Free memory: " + event.get("memory.free"));
            System.out.println("Active tasks: " + event.get("task.active"));
            System.out.println("Total tasks: " + event.get("task.total"));
        });
        // End:listener

        hekate.leave();
    }
}
