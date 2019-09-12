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

package io.hekate.cluster;

import io.hekate.core.jmx.JmxTypeName;
import javax.management.MXBean;

/**
 * JMX interface for {@link ClusterService}.
 */
@MXBean
@JmxTypeName("ClusterService")
public interface ClusterServiceJmx {
    /**
     * Returns the local node.
     *
     * @return Local node
     *
     * @see ClusterService#localNode()
     */
    ClusterNodeJmx getLocalNode();

    /**
     * Returns the current topology version.
     *
     * @return Topology version.
     *
     * @see ClusterTopology#version()
     */
    long getTopologyVersion();

    /**
     * Returns the current topology size.
     *
     * @return Topology size.
     *
     * @see ClusterTopology#size()
     */
    int getTopologySize();

    /**
     * Returns the current topology.
     *
     * @return Topology.
     */
    ClusterNodeJmx[] getTopology();
}
