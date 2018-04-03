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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.cloudwatch.model.StatisticSet;
import com.amazonaws.util.EC2MetadataUtils;
import io.hekate.core.Hekate;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.local.LocalMetricsServiceFactory;
import io.hekate.util.format.ToString;

/**
 * Configuration for {@link CloudWatchMetricsPlugin}.
 */
public class CloudWatchMetricsConfig {
    /** Default value (={@value}) in seconds for {@link #setPublishInterval(int)}. */
    public static final int DEFAULT_PUBLISH_INTERVAL = 60;

    /** Default value (={@value}) for {@link #setNamespace(String)}. */
    public static final String DEFAULT_NAMESPACE = "Hekate.io";

    /** See {@link #setNamespace(String)}. */
    private String namespace = DEFAULT_NAMESPACE;

    /** See {@link #setRegion(String)}. */
    private String region;

    /** See {@link #setPublishInterval(int)}. */
    private int publishInterval = DEFAULT_PUBLISH_INTERVAL;

    /** See {@link #setAccessKey(String)}. */
    private String accessKey;

    /** See {@link #setSecretKey(String)}. */
    private String secretKey;

    /** See {@link #setFilter(MetricFilter)}. */
    private MetricFilter filter;

    private CloudWatchMetaDataProvider metaDataProvider;

    /**
     * Returns the namespace for metrics publishing (see {@link #setNamespace(String)}).
     *
     * @return Namespace for for metrics publishing.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Sets the namespace for metrics publishing.
     *
     * <p>
     * Default value of this parameter is {@value #DEFAULT_NAMESPACE}
     * </p>
     *
     * @param namespace Namespace for for metrics publishing.
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Fluent-style version of {@link #setNamespace(String)}.
     *
     * @param namespace Namespace for for metrics publishing.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withNamespace(String namespace) {
        setNamespace(namespace);

        return this;
    }

    /**
     * Returns the AWS regions for metrics publishing (see {@link #setRegion(String)}).
     *
     * @return AWS region.
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the AWS regions for metrics publishing.
     *
     * <p>
     * This parameter is optional and if not specified then metrics will be published to the region of a EC2 instance that runs this
     * {@link Hekate} node (see {@link EC2MetadataUtils#getEC2InstanceRegion()}). If region can't be detected via {@link
     * EC2MetadataUtils#getEC2InstanceRegion()} then AWS SDK's default region will be used.
     * </p>
     *
     * @param region AWS region.
     */
    public void setRegion(String region) {
        this.region = region;
    }

    /**
     * Fluent-style version of {@link #setRegion(String)}.
     *
     * @param region AWS region.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withRegion(String region) {
        setRegion(region);

        return this;
    }

    /**
     * Returns the time interval in seconds for pushing metrics to AWS CloudWatch (see {@link #setPublishInterval(int)}).
     *
     * @return Time interval in seconds.
     */
    public int getPublishInterval() {
        return publishInterval;
    }

    /**
     * Sets the time interval in seconds for pushing metrics to AWS CloudWatch.
     *
     * <p>
     * Note that the value of this parameter can be larger than the value {@link LocalMetricsServiceFactory#setRefreshInterval(long) metrics
     * refresh interval} as all metrics are aggregated as {@link StatisticSet}s.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@link #DEFAULT_PUBLISH_INTERVAL}.
     * </p>
     *
     * @param publishInterval Time interval in seconds.
     */
    public void setPublishInterval(int publishInterval) {
        this.publishInterval = publishInterval;
    }

    /**
     * Fluent-style version of {@link #setPublishInterval(int)}.
     *
     * @param publishInterval Time interval in seconds.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withPublishInterval(int publishInterval) {
        setPublishInterval(publishInterval);

        return this;
    }

    /**
     * Returns the AWS access key (see {@link #setAccessKey(String)}).
     *
     * @return Access key.
     */
    public String getAccessKey() {
        return accessKey;
    }

    /**
     * Sets AWS access key.
     *
     * <p>
     * This parameter is optional and if not specified then credentials will be resolved via {@link DefaultAWSCredentialsProviderChain}.
     * </p>
     *
     * @param accessKey AWS access key.
     */
    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    /**
     * Fluent-style version of {@link #setAccessKey(String)}.
     *
     * @param accessKey AWS access key.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withAccessKey(String accessKey) {
        setAccessKey(accessKey);

        return this;
    }

    /**
     * Returns the AWS secret key (see {@link #setSecretKey(String)}).
     *
     * @return Secret key.
     */
    public String getSecretKey() {
        return secretKey;
    }

    /**
     * Sets AWS secret key.
     *
     * <p>
     * This parameter is optional and if not specified then credentials will be resolved via {@link DefaultAWSCredentialsProviderChain}.
     * </p>
     *
     * @param secretKey AWS secret key.
     */
    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }

    /**
     * Fluent-style version of {@link #setSecretKey(String)}.
     *
     * @param secretKey AWS secret key.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withSecretKey(String secretKey) {
        setSecretKey(secretKey);

        return this;
    }

    /**
     * Returns the metric filter (see {@link #setFilter(MetricFilter)}).
     *
     * @return Returns the metric filter.
     */
    public MetricFilter getFilter() {
        return filter;
    }

    /**
     * Sets the metric filter.
     *
     * <p>
     * If this parameter is specified then only those metrics that do match with the filter's criteria will be published to the CloudWatch
     * service.
     * </p>
     *
     * @param filter Metric filter.
     */
    public void setFilter(MetricFilter filter) {
        this.filter = filter;
    }

    /**
     * Fluent-style version of {@link #setFilter(MetricFilter)}.
     *
     * @param filter Metric filter.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withFilter(MetricFilter filter) {
        setFilter(filter);

        return this;
    }

    /**
     * Returns the EC2 meta-data provider (see {@link #setMetaDataProvider(CloudWatchMetaDataProvider)}).
     *
     * @return EC2 meta-data provider.
     */
    public CloudWatchMetaDataProvider getMetaDataProvider() {
        return metaDataProvider;
    }

    /**
     * Sets the EC2 meta-data provider.
     *
     * <p>
     * This parameter is optional and if not specified then {@link DefaultCloudWatchMetaDataProvider} will be used by default.
     * </p>
     *
     * @param metaDataProvider EC2 meta-data provider.
     */
    public void setMetaDataProvider(CloudWatchMetaDataProvider metaDataProvider) {
        this.metaDataProvider = metaDataProvider;
    }

    /**
     * Fluent-style version of {@link #setMetaDataProvider(CloudWatchMetaDataProvider)}.
     *
     * @param metaDataProvider EC2 meta-data provider.
     *
     * @return This instance.
     */
    public CloudWatchMetricsConfig withMetaDataProvider(CloudWatchMetaDataProvider metaDataProvider) {
        setMetaDataProvider(metaDataProvider);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
