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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.AddressUtils;
import io.hekate.core.plugin.Plugin;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.util.format.ToString;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * <span class="startHere">&laquo; start here</span>InfluxDB metrics publisher plugin.
 *
 * <h2>Overview</h2>
 * <p>
 * This plugin provides support for publishing metrics from {@link LocalMetricsService} to
 * <a href="https://www.influxdata.com" target="_blank">InfluxDB</a>.
 * </p>
 * <p>
 * Metrics are asynchronously published once per {@link LocalMetricsServiceFactory#setRefreshInterval(long)} interval. On each tick the
 * snapshot of current metrics is placed into a bounded queue of {@link InfluxDbMetricsConfig#setMaxQueueSize(int)} size. If queue is full
 * (i.e. publishing is slow due to some networking issues) then new metrics are dropped until there is more space in the queue.
 * </p>
 *
 * <h2>Module dependency</h2>
 * <p>
 * InfluxDB support is provided by the 'hekate-metrics-influxdb' module and can be imported into the project dependency management
 * system as in the example below:
 * </p>
 * <div class="tabs">
 * <ul>
 * <li><a href="#maven">Maven</a></li>
 * <li><a href="#gradle">Gradle</a></li>
 * <li><a href="#ivy">Ivy</a></li>
 * </ul>
 * <div id="maven">
 * <pre>{@code
 * <dependency>
 *   <groupId>io.hekate</groupId>
 *   <artifactId>hekate-metrics-influxdb</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-metrics-influxdb', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-metrics-influxdb" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * Configuration options of this plugin are represented by the {@link InfluxDbMetricsConfig} class. Please see the documentation of its
 * properties for more details.
 * </p>
 *
 * <h2>Registering plugin</h2>
 * <p>
 * This plugin can be registered via {@link HekateBootstrap#setPlugins(List)} method as in the example below:
 * </p>
 * <p>
 * 1) Prepare plugin configuration with InfluxDB connectivity options.
 * ${source: InfluxDbMetricsPluginJavadocTest.java#configure}
 * </p>
 * <p>
 * 2) Register plugin and start new node.
 * ${source: InfluxDbMetricsPluginJavadocTest.java#boot}
 * </p>
 *
 * <h2>Metrics names and tags</h2>
 * <p>
 * Metric {@link Metric#name() names} are escaped so that all characters that are not letters, digits or dots (.) are replaced with
 * underscores (_).
 * </p>
 *
 * <p>
 * During publishing each metric gets tagged with the following tags:
 * </p>
 *
 * <ul>
 * <li>{@value #NODE_ADDRESS_TAG} - contains the network address of a publisher node</li>
 * <li>{@value #NODE_NAME_TAG} - contains the name of a publisher node (see {@link ClusterNode#name()})</li>
 * </ul>
 *
 * <h2>Metrics filtering</h2>
 * <p>
 * It is possible to filter out metrics that should not be published to InfluxDB by {@link InfluxDbMetricsConfig#setFilter(MetricFilter)
 * registering} an instance of {@link MetricFilter} interface. Only those metrics that do match the specified filter will be published to
 * InfluxDB.
 * </p>
 *
 * @see InfluxDbMetricsConfig
 * @see HekateBootstrap#setPlugins(List)
 */
public class InfluxDbMetricsPlugin implements Plugin {
    /** Name of a tag ({@value}) that gets attached to each metric and holds the name of a publishing node. */
    public static final String NODE_NAME_TAG = "node_name";

    /** Name of a tag ({@value}) that gets attached to each metric and holds the network address of a publishing node. */
    public static final String NODE_ADDRESS_TAG = "node_address";

    /** Name of a field ({@value}) to store the metric value (see {@link Metric#value()}). */
    public static final String METRIC_VALUE_FIELD = "value";

    private final InfluxDbMetricsPublisher publisher;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public InfluxDbMetricsPlugin(InfluxDbMetricsConfig cfg) {
        publisher = new InfluxDbMetricsPublisher(cfg);
    }

    @Override
    public void install(HekateBootstrap boot) {
        boot.withService(LocalMetricsServiceFactory.class, metrics ->
            metrics.withListener(event ->
                publisher.publish(event.allMetrics().values())
            )
        );
    }

    @Override
    public void start(Hekate hekate) throws HekateException {
        ClusterNode node = hekate.localNode();

        InetSocketAddress address = node.socket();

        publisher.start(node.name(), AddressUtils.host(address), address.getPort());
    }

    @Override
    public void stop() throws HekateException {
        publisher.stop();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
