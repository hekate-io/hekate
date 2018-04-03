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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.cloudwatch.AmazonCloudWatch;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClientBuilder;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateBootstrap;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.core.internal.util.Utils;
import io.hekate.core.plugin.Plugin;
import io.hekate.metrics.Metric;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.cloudwatch.CloudWatchMetricsPublisher.CloudWatchClient;
import io.hekate.metrics.local.LocalMetricsService;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <span class="startHere">&laquo; start here</span>Amazon CloudWatch metrics publisher plugin.
 *
 * <h2>Overview</h2>
 * <p>
 * This plugin provides support for publishing metrics from {@link LocalMetricsService} to
 * <a href="https://aws.amazon.com/cloudwatch/" target="_blank">Amazon CloudWatch</a>.
 * </p>
 * <p>
 * Metrics are asynchronously published once per {@link CloudWatchMetricsConfig#setPublishInterval(int)} interval as {@link StatisticSet}s.
 * Each {@link StatisticSet} contains average values that were aggregated during that interval.
 * </p>
 *
 * <p>
 * <b>Important! </b> Publishing metrics the Amazon CloudWatch service requires {@code cloudwatch:PutMetricData} permission to be set
 * for the EC2 instance's IAM role.
 * </p>
 *
 * <h2>Module Dependency</h2>
 * <p>
 * CloudWatch support is provided by the 'hekate-metrics-cloudwatch' module and can be imported into the project dependency management
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
 *   <artifactId>hekate-metrics-cloudwatch</artifactId>
 *   <version>REPLACE_VERSION</version>
 * </dependency>
 * }</pre>
 * </div>
 * <div id="gradle">
 * <pre>{@code
 * compile group: 'io.hekate', name: 'hekate-metrics-cloudwatch', version: 'REPLACE_VERSION'
 * }</pre>
 * </div>
 * <div id="ivy">
 * <pre>{@code
 * <dependency org="io.hekate" name="hekate-metrics-cloudwatch" rev="REPLACE_VERSION"/>
 * }</pre>
 * </div>
 * </div>
 *
 * <h2>Configuration</h2>
 * <p>
 * Configuration options of this plugin are represented by the {@link CloudWatchMetricsConfig} class. Please see the documentation of its
 * properties for more details.
 * </p>
 *
 * <h2>Registering Plugin</h2>
 * <p>
 * This plugin can be registered via {@link HekateBootstrap#setPlugins(List)} method as in the example below:
 * </p>
 * <p>
 * 1) Prepare plugin configuration.
 * ${source: CloudWatchMetricsPluginJavadocTest.java#configure}
 * </p>
 * <p>
 * 2) Register the plugin and start a new node.
 * ${source: CloudWatchMetricsPluginJavadocTest.java#boot}
 * </p>
 *
 * <h2>Metrics Names and Dimensions</h2>
 * <p>
 * Metric {@link Metric#name() names} are converted to camel case. For example, if metric name is {@code jvm.mem.used} then it will be
 * published as {@code JvmMemUsed}
 * </p>
 *
 * <p>
 * During publishing each metric gets tagged with the following dimensions:
 * </p>
 *
 * <ul>
 * <li>{@code NodeName} - Name of a publisher node (see {@link ClusterNode#name()})</li>
 * <li>{@code InstanceId} - EC2 instance ID</li>
 * </ul>
 *
 * <h2>Metrics Filtering</h2>
 * <p>
 * It is possible to filter out metrics that should not be published to CloudWatch by
 * {@link CloudWatchMetricsConfig#setFilter(MetricFilter) registering} an instance of {@link MetricFilter} interface. Only those
 * metrics that do match the specified filter will be published to CloudWatch.
 * </p>
 *
 * @see CloudWatchMetricsConfig
 * @see HekateBootstrap#setPlugins(List)
 */
public class CloudWatchMetricsPlugin implements Plugin {
    private final CloudWatchMetricsPublisher publisher;

    /**
     * Constructs new instance.
     *
     * @param cfg Configuration.
     */
    public CloudWatchMetricsPlugin(CloudWatchMetricsConfig cfg) {
        ArgAssert.notNull(cfg, "Configuration");

        int interval = cfg.getPublishInterval();
        String namespace = cfg.getNamespace();
        MetricFilter metricFilter = cfg.getFilter();

        ConfigCheck check = ConfigCheck.get(CloudWatchMetricsConfig.class);

        check.positive(interval, "publish interval");
        check.notEmpty(namespace, "namespace");

        // Resolve instance meta-data.
        CloudWatchMetaDataProvider metaData = cfg.getMetaDataProvider();

        if (metaData == null) {
            metaData = new DefaultCloudWatchMetaDataProvider();
        }

        String ec2Region = metaData.getInstanceRegion();
        String ec2InstanceId = metaData.getInstanceId();

        String region = resolveRegion(cfg.getRegion(), ec2Region);

        // Prepare CloudWatch client.
        AmazonCloudWatchClientBuilder cloudWatchBuilder = AmazonCloudWatchClientBuilder.standard();

        // Resolve credentials.
        String accessKey = cfg.getAccessKey();
        String secretKey = cfg.getSecretKey();

        if (accessKey != null && !accessKey.trim().isEmpty() && secretKey != null && !secretKey.trim().isEmpty()) {
            // Use pre-configured credentials.
            cloudWatchBuilder.withCredentials(
                new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(
                        accessKey.trim(),
                        secretKey.trim()
                    )
                )
            );
        }

        if (region != null) {
            cloudWatchBuilder.withRegion(region);
        }

        AmazonCloudWatch cloudWatch = cloudWatchBuilder.build();

        CloudWatchClient cloudWatchClient = buildCloudWatchClient(cloudWatch);

        // Build publisher.
        publisher = new CloudWatchMetricsPublisher(
            TimeUnit.SECONDS.toMillis(interval),
            namespace,
            ec2InstanceId,
            metricFilter,
            cloudWatchClient
        );
    }

    @Override
    public void install(HekateBootstrap boot) {
        boot.withService(LocalMetricsServiceFactory.class, metrics ->
            metrics.withListener(event -> {
                if (!event.allMetrics().isEmpty()) {
                    publisher.publish(event.allMetrics().values());
                }
            })
        );
    }

    @Override
    public void start(Hekate hekate) throws HekateException {
        publisher.start(hekate.localNode().name());
    }

    @Override
    public void stop() throws HekateException {
        publisher.stop();
    }

    // Package level for testing purposes.
    CloudWatchClient buildCloudWatchClient(AmazonCloudWatch cloudWatch) {
        return cloudWatch::putMetricData;
    }

    private String resolveRegion(String preConfigured, String metaDataRegion) {
        preConfigured = Utils.nullOrTrim(preConfigured);
        metaDataRegion = Utils.nullOrTrim(metaDataRegion);

        return preConfigured == null ? metaDataRegion : preConfigured;
    }
}
