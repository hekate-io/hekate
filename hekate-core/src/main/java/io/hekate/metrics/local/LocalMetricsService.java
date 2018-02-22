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

import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.DefaultServiceFactory;
import io.hekate.core.service.Service;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricsSource;
import io.hekate.metrics.cluster.ClusterMetricsService;

/**
 * <span class="startHere">&laquo; start here</span>Main entry point to node-local metrics API.
 *
 * <h2>Overview</h2>
 * <p>
 * This service provides support for managing metrics of a running {@link Hekate} node with ability to dynamically register custom
 * application-specific metrics. The following types of metrics are supported by this service:
 * </p>
 * <ul>
 * <li><a href="#counters">Counters</a> - metrics with incrementing/decrementing values. </li>
 * <li><a href="#probes">Probes</a> - metrics that obtain their values from some third-party source.</li>
 * <li><a href="#timers">Timers</a> - for tracking durations of arbitrary events.</li>
 * </ul>
 *
 * <p>
 * All metrics that are registered within this service can be exposed to remote cluster nodes with the help of
 * {@link ClusterMetricsService}. Please see its documentation for more details.
 * </p>
 *
 * <h2>Service configuration</h2>
 * <p>
 * Metrics service can be configured and registered in {@link HekateBootstrap} with the help of {@link LocalMetricsServiceFactory} as shown
 * in the example below:
 * </p>
 *
 * <div class="tabs">
 * <ul>
 * <li><a href="#configure-java">Java</a></li>
 * <li><a href="#configure-xsd">Spring XSD</a></li>
 * <li><a href="#configure-bean">Spring bean</a></li>
 * </ul>
 * <div id="configure-java">
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#configure}
 * </div>
 * <div id="configure-xsd">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: metrics/local/service-xsd.xml#example}
 * </div>
 * <div id="configure-bean">
 * <b>Note:</b> This example requires Spring Framework integration
 * (see <a href="{@docRoot}/io/hekate/spring/bean/HekateSpringBootstrap.html">HekateSpringBootstrap</a>).
 * ${source: metrics/local/service-bean.xml#example}
 * </div>
 * </div>
 *
 * <p>
 * For all available configuration options please see the documentation of {@link LocalMetricsServiceFactory} class.
 * </p>
 *
 * <h2>Accessing service</h2>
 * <p>
 * Metrics service can be accessed via {@link Hekate#localMetrics()} method as in the example below:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#access}
 * </p>
 *
 * <a name="counters"></a>
 * <h2>Counters</h2>
 * <p>
 * Counters are typically used to track custom application statistics like the amount of processed transactions, active connections or
 * service requests. Basically everything that can be represented as a dynamically increasing (or decreasing) value falls into the category
 * of counter metrics.
 * </p>
 *
 * <p>
 * Counters can be registered within the {@link LocalMetricsService} either at
 * {@link LocalMetricsServiceFactory#withMetric(MetricConfigBase) configuration time} or {@link LocalMetricsService#register(CounterConfig)
 * dynamically} at runtime. Once registered they can be accessed via the {@link LocalMetricsService#counter(String)} method.
 * </p>
 *
 * <p>
 * Configuration of each counter is represented by the {@link CounterConfig} class with the following key properties:
 * </p>
 * <ul>
 * <li>{@link CounterConfig#setName(String) Metric name} - unique name of the counter;</li>
 * <li>{@link CounterConfig#setAutoReset(boolean) Auto-reset} - enabled/disables auto-resetting of the counter to its default value
 * after every {@link LocalMetricsServiceFactory#setRefreshInterval(long) refresh interval};</li>
 * </ul>
 *
 * <p>
 * For more information about configuration options please see the documentation of {@link CounterConfig} class.
 * </p>
 *
 * <p>
 * The code example below illustrates the basic usage of counter for tracking task executions in some imaginary service:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#counter_example}
 * </p>
 * <p>
 * ... and access those values in some other components of the program:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#counter_example_usage}
 * </p>
 *
 * <a name="probes"></a>
 * <h2>Probes</h2>
 * <p>
 * Probes provide metrics based on periodic polling of some third-party sources. Examples of such sources are current CPU utilization,
 * memory consumption or availability status of some service.
 * </p>
 *
 * <p>
 * Probes can be registered within the {@link LocalMetricsService} either at {@link LocalMetricsServiceFactory#withMetric(MetricConfigBase)
 * configuration time} or dynamically at {@link LocalMetricsService#register(ProbeConfig) runtime}.
 * </p>
 *
 * <p>
 * Configuration of each probe is represented by the {@link ProbeConfig} class with the following key properties:
 * </p>
 * <ul>
 * <li>{@link ProbeConfig#setName(String) Metric name} - unique name of the probe;</li>
 * <li>{@link ProbeConfig#setProbe(Probe) Probe} - implementation of {@link Probe} interface that is responsible for
 * obtaining the probe value from the third-party source;</li>
 * <li>{@link ProbeConfig#setInitValue(long) Initial value} - (optional) initial value that the probe should start with;</li>
 * </ul>
 *
 * <p>
 * For more information about configuration options please see the documentation of {@link ProbeConfig} class.
 * </p>
 *
 * <p>
 * Once probe is registered, {@link LocalMetricsService} will start polling it in order to obtain and cache the latest value. Polling
 * interval is controlled by the {@link LocalMetricsServiceFactory#setRefreshInterval(long)} configuration option.
 * </p>
 *
 * <p>
 * The code example below illustrates the basic implementation of the {@link Probe} interface:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#probe_example}
 * </p>
 *
 * <a name="timers"></a>
 * <h2>Timers</h2>
 * <p>
 * Timers measure the amount of time (in nanoseconds) it takes to process an arbitrary operation and can optionally provide a
 * {@link TimerConfig#setRateName(String) rate} of that operation per {@link LocalMetricsServiceFactory#setRefreshInterval(long) refresh
 * interval}.
 * </p>
 *
 * <p>
 * Timers can be registered within the {@link LocalMetricsService} either {@link LocalMetricsServiceFactory#withMetric(MetricConfigBase)
 * statically} or {@link LocalMetricsService#register(TimerConfig) dynamically}. Once registered they can be accessed via
 * the {@link LocalMetricsService#timer(String)} method.
 * </p>
 *
 * <p>
 * Configuration of each timer is represented by the {@link TimerConfig} class with the following key properties:
 * </p>
 * <ul>
 * <li>{@link TimerConfig#setName(String) Timer metric name} - unique name of the timer;</li>
 * <li>{@link TimerConfig#setRateName(String) Rate metric name} - (optional) name of a metric that will hold the rate value of this
 * timer;</li>
 * </ul>
 *
 * <p>
 * For more information about configuration options please see the documentation of {@link TimerConfig} class.
 * </p>
 *
 * <p>
 * The following code example shows the basic use of a timer to track the time/rate of execution in some imaginary service:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#timer_example}
 * </p>
 * <p>
 * ... and access those values in some other components of the program:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#timer_example_usage}
 * </p>
 *
 * <h2>Accessing metrics</h2>
 * <p>
 * {@link LocalMetricsService} extends the {@link MetricsSource} interface which provides methods for accessing all registered metrics by
 * their names.
 * </p>
 *
 * <p>
 * Another approach for accessing metrics is to {@link #addListener(MetricsListener) register} an instance of {@link
 * MetricsListener} interface. Such listener will be notified once per {@link LocalMetricsServiceFactory#getRefreshInterval() refresh
 * interval} with a snapshot of metric values. Below is the example of simple listener that prints out metrics from previous examples:
 * ${source: metrics/local/LocalMetricsServiceJavadocTest.java#listener}
 * </p>
 *
 * <h2>Default metrics</h2>
 * <p>
 * By default the following metrics are available out of the box:
 * </p>
 * <ul>
 * <li><b>jvm.mem.used</b> - The amount of memory in megabytes used by the JVM</li>
 * <li><b>jvm.mem.committed</b> - The amount of memory in megabytes that is committed for the JVM to use</li>
 * <li><b>jvm.mem.free</b> - The amount of memory in megabytes that is available to the JVM</li>
 * <li><b>jvm.mem.max</b> - The maximum amount of memory in megabytes that the JVM will attempt to use</li>
 * <li><b>jvm.mem.heap.committed</b> - The amount of heap memory in megabytes that is committed for the JVM to use</li>
 * <li><b>jvm.mem.heap.used</b> - The amount of heap memory in megabytes used by the JVM</li>
 * <li><b>jvm.mem.nonheap.committed</b> - The amount of non-heap memory in megabytes that is committed for the JVM to use</li>
 * <li><b>jvm.mem.nonheap.used</b> - The amount of non-heap memory in megabytes used by the JVM</li>
 * <li><b>jvm.threads.live</b> - The current number of live threads including both daemon and non-daemon threads</li>
 * <li><b>jvm.threads.daemon</b> - The current number of live daemon threads</li>
 * <li><b>jvm.cpu.count</b> - The the number of processors available to the JVM</li>
 * <li><b>jvm.cpu.load</b> - The system CPU load average in percents (0-100) for the last minute or -1 if such metric is not supported by
 * the JVM.</li>
 * </ul>
 */
@DefaultServiceFactory(LocalMetricsServiceFactory.class)
public interface LocalMetricsService extends Service, MetricsSource {
    /**
     * Returns an immutable snapshot of metrics that were aggregated by this service.
     *
     * <p>
     * Note that snapshots get aggregated once per {@link LocalMetricsServiceFactory#setRefreshInterval(long)}.
     * </p>
     *
     * @return Metrics snapshot.
     */
    MetricsSnapshot snapshot();

    /**
     * Registers a new counter with the specified configuration or returns an existing one if counter with the same name already exists.
     * If there is another metric of a different type then an error will be thrown.
     *
     * @param config Counter configuration.
     *
     * @return Counter.
     */
    CounterMetric register(CounterConfig config);

    /**
     * Registers a new timer with the specified configuration or returns an existing one if timer with the same name already exists.
     * If there is another metric of a different type then an error will be thrown.
     *
     * @param config Timer configuration.
     *
     * @return Timer.
     */
    TimerMetric register(TimerConfig config);

    /**
     * Registers a probe with the specified configuration.
     * If there is another metric with the same {@link  ProbeConfig#setName(String) name} then an error will be thrown.
     *
     * @param config Probe configuration.
     *
     * @return Metric for the new newly created probe.
     */
    Metric register(ProbeConfig config);

    /**
     * Returns a counter for the specified name or register a new counter with the default {@link CounterConfig configuration} if there is
     * no such counter.
     *
     * @param name Counter name (see {@link CounterConfig#setName(String)}).
     *
     * @return Counter.
     */
    CounterMetric counter(String name);

    /**
     * Returns a timer for the specified name or register a new timer with the default {@link TimerConfig configuration} if there is
     * no such timer.
     *
     * @param name Timer name (see {@link TimerConfig#setName(String)}).
     *
     * @return Timer.
     */
    TimerMetric timer(String name);

    /**
     * Registers the specified listener.
     *
     * <p>
     * Listener will be notified once per {@link LocalMetricsServiceFactory#getRefreshInterval()}.
     * </p>
     *
     * <p>
     * <b>Note:</b> listeners are notified on the same thread that performs metrics management and hence should process notifications as
     * fast as possible. If notification processing is a time consuming task then it is highly recommended to offload such task to some
     * other thread and process it asynchronously.
     * </p>
     *
     * @param listener Listener.
     */
    void addListener(MetricsListener listener);

    /**
     * Unregisters the specified listener.
     *
     * @param listener Listener.
     */
    void removeListener(MetricsListener listener);

    /**
     * Returns the configuration value of {@link LocalMetricsServiceFactory#getRefreshInterval()}.
     *
     * @return Value of {@link LocalMetricsServiceFactory#getRefreshInterval()}.
     */
    long refreshInterval();
}
