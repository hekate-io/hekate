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

package io.hekate.rpc;

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxTypeName;
import java.util.Set;
import javax.management.MXBean;

/**
 * JMX interface for {@link RpcServerConfig}.
 *
 * <p>
 * Instances of this interface are registered to the {@link JmxService} for each combination of {@link RpcInterfaceInfo RPC interface}
 * (that is implemented by the {@link RpcServerConfig#setHandler(Object) RPC handler}) and each {@link RpcServerConfig#setTags(Set) tag}
 * (if specified)
 * </p>
 */
@MXBean
@JmxTypeName("RpcServer")
public interface RpcServerJmx {
    /**
     * Returns the class name of the server's {@link RpcInterfaceInfo#javaType() RPC interface}.
     *
     * @return Class name of the server's RPC interface.
     *
     * @see RpcInterfaceInfo#javaType()
     */
    String getInterfaceType();

    /**
     * Returns the value of {@link RpcInterfaceInfo#version()}.
     *
     * @return Value of {@link RpcInterfaceInfo#version()}.
     */
    int getInterfaceVersion();

    /**
     * Returns the value of {@link RpcInterfaceInfo#minClientVersion()}.
     *
     * @return Value of {@link RpcInterfaceInfo#minClientVersion()}.
     */
    int getInterfaceMinClientVersion();

    /**
     * Returns the class name of the servers's handler (see {@link RpcServerConfig#setHandler(Object)}).
     *
     * @return Class name of the servers's handler (see {@link RpcServerConfig#setHandler(Object)}).
     */
    String getHandlerType();

    /**
     * Returns the {@link RpcServerConfig#setTags(Set) tag} of this server or {@code null} if server doesn't have tags.
     *
     * @return {@link RpcServerConfig#setTags(Set) Tag} of this server or {@code null} if server doesn't have tags.
     */
    String getTag();

    /**
     * Returns the cluster topology of this server (i.e. all nodes that have a server for the same combination of {@link RpcInterfaceInfo}
     * and {@link #getTag()})
     *
     * @return Cluster topology of this server.
     */
    ClusterNodeJmx[] getTopology();
}
