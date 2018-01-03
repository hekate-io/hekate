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

package io.hekate.metrics.cluster;

import io.hekate.core.HekateBootstrap;
import io.hekate.core.service.ServiceFactory;
import io.hekate.metrics.MetricFilter;
import io.hekate.metrics.cluster.internal.DefaultClusterMetricsService;
import io.hekate.util.format.ToString;

/**
 * Factory for {@link ClusterMetricsService}.
 *
 * <p>
 * This class represents a configurable factory for {@link ClusterMetricsService}. Instances of this class can be
 * {@link HekateBootstrap#withService(ServiceFactory) registered} within the {@link HekateBootstrap} in order to customize options of the
 * {@link ClusterMetricsService}.
 * </p>
 *
 * <p>
 * For more details about the {@link ClusterMetricsService} and its capabilities please see the documentation of {@link
 * ClusterMetricsService} interface.
 * </p>
 *
 * @see ClusterMetricsService
 */
public class ClusterMetricsServiceFactory implements ServiceFactory<ClusterMetricsService> {
    /** Default {@value} in milliseconds for {@link #setReplicationInterval(long)}. */
    public static final long DEFAULT_REPLICATION_INTERVAL = 1000;

    private long replicationInterval = DEFAULT_REPLICATION_INTERVAL;

    private MetricFilter replicationFilter;

    private boolean enabled;

    /**
     * Returns {@code true} if cluster metrics should be enabled on this node (see {@link #setEnabled(boolean)}).
     *
     * @return {@code true} if cluster metrics should be enabled.
     */
    public boolean isEnabled() {
        return enabled;
    }

    /**
     * Sets the flag indicating whether cluster metrics should be enabled or disabled.
     *
     * <p>
     * If disabled, then this node will not be able to see metrics of other nodes and other nodes will not be able to see metrics of this
     * node. Default value of this parameter is {@code false}.
     * </p>
     *
     * @param enabled {@code true} if cluster metrics should be enabled.
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * Fluent-style version of {@link #setEnabled(boolean)}.
     *
     * @param enabled {@code true} if cluster metrics should be enabled.
     *
     * @return This instance.
     */
    public ClusterMetricsServiceFactory withEnabled(boolean enabled) {
        setEnabled(enabled);

        return this;
    }

    /**
     * Returns the time interval in milliseconds for metrics replication over the cluster {@link #setReplicationInterval(long)}.
     *
     * @return Time interval in milliseconds.
     */
    public long getReplicationInterval() {
        return replicationInterval;
    }

    /**
     * Sets the time interval in milliseconds for metrics replication over the cluster.
     *
     * <p>
     * Value of this parameter must be above zero. Default value if {@value #DEFAULT_REPLICATION_INTERVAL}.
     * </p>
     *
     * @param replicationInterval Time interval in milliseconds.
     */
    public void setReplicationInterval(long replicationInterval) {
        this.replicationInterval = replicationInterval;
    }

    /**
     * Fluent-style version of {@link #setReplicationInterval(long)}.
     *
     * @param replicationInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public ClusterMetricsServiceFactory withReplicationInterval(long replicationInterval) {
        setReplicationInterval(replicationInterval);

        return this;
    }

    /**
     * Returns the metrics replication filter (see {@link #setReplicationFilter(MetricFilter)}).
     *
     * @return Metrics replication filter.
     */
    public MetricFilter getReplicationFilter() {
        return replicationFilter;
    }

    /**
     * Set the metrics replication filter. Only those metrics that match the specified filter will be published by the local node to other
     * cluster members.
     *
     * @param replicationFilter Replication filter.
     */
    public void setReplicationFilter(MetricFilter replicationFilter) {
        this.replicationFilter = replicationFilter;
    }

    /**
     * Fluent-style version of {@link #setReplicationFilter(MetricFilter)}.
     *
     * @param replicationFilter Replication filter.
     *
     * @return This instance.
     */
    public ClusterMetricsServiceFactory withReplicationFilter(MetricFilter replicationFilter) {
        setReplicationFilter(replicationFilter);

        return this;
    }

    @Override
    public ClusterMetricsService createService() {
        return new DefaultClusterMetricsService(this);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
