/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.cluster.health;

import io.hekate.util.format.ToString;
import java.net.ConnectException;

/**
 * Configuration for {@link DefaultFailureDetector}.
 *
 * @see DefaultFailureDetector
 */
public class DefaultFailureDetectorConfig {
    /** Default value (={@value}) for {@link #setHeartbeatInterval(long)}. */
    public static final int DEFAULT_HEARTBEAT_INTERVAL = 500;

    /** Default value (={@value}) for {@link #setHeartbeatLossThreshold(int)}. */
    public static final int DEFAULT_HEARTBEAT_LOSS_THRESHOLD = 6;

    /** Default value (={@value}) for {@link #setFailureDetectionQuorum(int)}. */
    public static final int DEFAULT_FAILURE_DETECTION_QUORUM = 2;

    private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;

    private int heartbeatLossThreshold = DEFAULT_HEARTBEAT_LOSS_THRESHOLD;

    private int failureDetectionQuorum = DEFAULT_FAILURE_DETECTION_QUORUM;

    private boolean failFast = true;

    /**
     * Returns the heartbeat sending interval in milliseconds (see {@link #setHeartbeatInterval(long)}).
     *
     * @return Time interval in milliseconds.
     */
    public long getHeartbeatInterval() {
        return heartbeatInterval;
    }

    /**
     * Sets the heartbeat sending interval in milliseconds.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_HEARTBEAT_INTERVAL}.
     * </p>
     *
     * @param heartbeatInterval Time interval in milliseconds.
     */
    public void setHeartbeatInterval(long heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }

    /**
     * Fluent-style version of {@link #setHeartbeatInterval(long)}.
     *
     * @param heartbeatInterval Time interval in milliseconds.
     *
     * @return This instance.
     */
    public DefaultFailureDetectorConfig withHeartbeatInterval(long heartbeatInterval) {
        setHeartbeatInterval(heartbeatInterval);

        return this;
    }

    /**
     * Returns the amount of heartbeats that can be lost before considering a node failure (see {@link #setHeartbeatLossThreshold(int)}).
     *
     * @return Heartbeat loss threshold.
     */
    public int getHeartbeatLossThreshold() {
        return heartbeatLossThreshold;
    }

    /**
     * Sets the amount of heartbeats that can be lost before considering a node failure.
     *
     * <p>
     * Value of this parameter must be above zero. Default value is {@value #DEFAULT_HEARTBEAT_LOSS_THRESHOLD}.
     * </p>
     *
     * @param heartbeatLossThreshold Heartbeat loss threshold.
     */
    public void setHeartbeatLossThreshold(int heartbeatLossThreshold) {
        this.heartbeatLossThreshold = heartbeatLossThreshold;
    }

    /**
     * Fluent-style version of {@link #setHeartbeatLossThreshold(int)}.
     *
     * @param heartbeatLossThreshold Heartbeat loss threshold.
     *
     * @return This instance.
     */
    public DefaultFailureDetectorConfig withHeartbeatLossThreshold(int heartbeatLossThreshold) {
        setHeartbeatLossThreshold(heartbeatLossThreshold);

        return this;
    }

    /**
     * Returns the amount of nodes that should agree on some particular node failure before removing such suspected node from the cluster
     * (see {@link #setFailureDetectionQuorum(int)}).
     *
     * @return Amount of nodes that should agree on some particular node failure before removing such suspected node from the cluster.
     */
    public int getFailureDetectionQuorum() {
        return failureDetectionQuorum;
    }

    /**
     * Sets the amount of nodes that should agree on some particular node failure before removing such suspected node from the cluster.
     *
     * <p>
     * Value of this parameter must be above 1. Default value is {@value #DEFAULT_FAILURE_DETECTION_QUORUM}.
     * </p>
     *
     * @param failureDetectionQuorum Amount of nodes that should agree on some particular node failure before removing such suspected node
     * from the cluster.
     *
     * @see FailureDetector#failureQuorum()
     */
    public void setFailureDetectionQuorum(int failureDetectionQuorum) {
        this.failureDetectionQuorum = failureDetectionQuorum;
    }

    /**
     * Fluent-style version of {@link #setFailureDetectionQuorum(int)}.
     *
     * @param failureDetectionQuorum Amount of nodes that should agree on some particular node failure before removing such suspected node
     * from the cluster.
     *
     * @return This instance.
     */
    public DefaultFailureDetectorConfig withFailureDetectionQuorum(int failureDetectionQuorum) {
        setFailureDetectionQuorum(failureDetectionQuorum);

        return this;
    }

    /**
     * Sets the flag that controls the failure detection behavior in case of network connectivity issues between the cluster nodes (see
     * {@link #setFailFast(boolean)}.
     *
     * @return {@code true} if fast-fail failure detection mode is enabled.
     */
    public boolean isFailFast() {
        return failFast;
    }

    /**
     * Sets the flag that controls the failure detection behavior in case of network connectivity issues between the cluster nodes.
     *
     * <p>
     * If this flag is set to {@code true} then cluster node will be immediately treated as failed in case of a {@link ConnectException}
     * while sending a gossip message to that node. If set to {@code false} then failure detector will wait for all
     * {@link #setHeartbeatLossThreshold(int)} attempts before treating such node as failed.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code true}.
     * </p>
     *
     * @param failFast {@code true} to speed up failure detection.
     */
    public void setFailFast(boolean failFast) {
        this.failFast = failFast;
    }

    /**
     * Fluent-style version of {@link #setFailFast(boolean)}.
     *
     * @param failFast {@code true} to speed up failure detection.
     *
     * @return This instance.
     */
    public DefaultFailureDetectorConfig withFailFast(boolean failFast) {
        setFailFast(failFast);

        return this;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
