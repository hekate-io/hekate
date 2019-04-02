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

package io.hekate.core;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.core.jmx.JmxTypeName;
import java.net.SocketAddress;
import javax.management.MXBean;

/**
 * JMX interface for {@link Hekate} instance.
 */
@MXBean
@JmxTypeName("Hekate")
public interface HekateJmx {
    /**
     * Returns the {@link HekateVersion#fullVersion()}  string.
     *
     * @return {@link HekateVersion#fullVersion()}  string.
     */
    String getVersion();

    /**
     * Returns the cluster name.
     *
     * @return Cluster name.
     *
     * @see HekateBootstrap#setClusterName(String)
     */
    String getClusterName();

    /**
     * Returns the local node's name.
     *
     * @return Name of the local node.
     *
     * @see HekateBootstrap#setNodeName(String)
     */
    String getNodeName();

    /**
     * Returns the string representation of the local node's {@link ClusterNodeId}.
     *
     * @return String representation of the local node's {@link ClusterNodeId}.
     *
     * @see ClusterNode#id()
     */
    String getNodeId();

    /**
     * Returns the local node's host address.
     *
     * @return Host address of the local node.
     */
    String getHost();

    /**
     * Returns the local node's TCP port.
     *
     * @return TCP port of the local node.
     */
    int getPort();

    /**
     * Returns the string representation of the local node's {@link SocketAddress}.
     *
     * @return String representation of the local node's {@link SocketAddress}
     *
     * @see ClusterNode#socket()
     */
    String getSocketAddress();

    /**
     * Returns the local node's {@link Hekate.State state}.
     *
     * @return Local node's {@link Hekate.State state}.
     *
     * @see Hekate#state()
     */
    Hekate.State getState();

    /**
     * Returns the number of milliseconds since the local node switched to the {@link Hekate.State#UP} state. Returns {@code 0} if instance
     * is not {@link Hekate.State#UP}.
     *
     * @return Number of milliseconds since the local node switched to the {@link Hekate.State#UP} state. Returns {@code 0} if instance
     * is not {@link Hekate.State#UP}.
     */
    long getUpTimeMillis();

    /**
     * Returns the formatted value of {@link #getUpTimeMillis()}.
     *
     * @return Formatted value of {@link #getUpTimeMillis()}.
     */
    String getUpTime();
}
