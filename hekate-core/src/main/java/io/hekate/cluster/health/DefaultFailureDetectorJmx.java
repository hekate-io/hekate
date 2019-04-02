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

import io.hekate.cluster.ClusterAddress;
import io.hekate.core.jmx.JmxTypeName;
import java.util.List;
import javax.management.MXBean;

/**
 * JMX interface for {@link DefaultFailureDetector}.
 */
@MXBean
@JmxTypeName("DefaultFailureDetector")
public interface DefaultFailureDetectorJmx {
    /**
     * Returns the value of {@link DefaultFailureDetectorConfig#setHeartbeatInterval(long)}.
     *
     * @return Value of {@link DefaultFailureDetectorConfig#setHeartbeatInterval(long)}.
     */
    long getHeartbeatInterval();

    /**
     * Returns the value of {@link DefaultFailureDetectorConfig#setHeartbeatLossThreshold(int)}.
     *
     * @return Value of {@link DefaultFailureDetectorConfig#setHeartbeatLossThreshold(int)}
     */
    int getHeartbeatLossThreshold();

    /**
     * Returns the value of {@link DefaultFailureDetectorConfig#setFailureDetectionQuorum(int)} .
     *
     * @return Value of {@link DefaultFailureDetectorConfig#setFailureDetectionQuorum(int)}
     */
    int getFailureDetectionQuorum();

    /**
     * Returns the value of {@link DefaultFailureDetector#monitored()} as a list of {@link ClusterAddress#toString() strings}.
     *
     * @return Value of {@link DefaultFailureDetector#monitored()} as a list of {@link ClusterAddress#toString() strings}.
     *
     * @see ClusterAddress#toString()
     */
    List<String> getMonitoredAddresses();
}
