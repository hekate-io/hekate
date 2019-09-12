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

package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterNodeJmx;
import io.hekate.cluster.ClusterView;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcServerInfo;
import io.hekate.rpc.RpcServerJmx;

class DefaultRpcServerJmx implements RpcServerJmx {
    private final RpcInterfaceInfo<?> rpcInterface;

    private final String tag;

    private final RpcServerInfo server;

    private final ClusterView cluster;

    public DefaultRpcServerJmx(RpcInterfaceInfo<?> rpcInterface, String tag, RpcServerInfo server, ClusterView cluster) {
        assert rpcInterface != null : "RPC interface is null.";
        assert server != null : "Server is null.";
        assert cluster != null : "Cluster is null.";

        this.rpcInterface = rpcInterface;
        this.tag = tag;
        this.server = server;
        this.cluster = cluster;
    }

    @Override
    public String getInterfaceType() {
        return rpcInterface.name();
    }

    @Override
    public int getInterfaceVersion() {
        return rpcInterface.version();
    }

    @Override
    public int getInterfaceMinClientVersion() {
        return rpcInterface.minClientVersion();
    }

    @Override
    public String getHandlerType() {
        return server.rpc().getClass().getName();
    }

    @Override
    public String getTag() {
        return tag;
    }

    @Override
    public ClusterNodeJmx[] getTopology() {
        return cluster.topology().stream().map(ClusterNodeJmx::of).toArray(ClusterNodeJmx[]::new);
    }
}
