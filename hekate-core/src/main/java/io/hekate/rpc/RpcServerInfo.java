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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.StreamUtils;
import io.hekate.util.format.ToString;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Provides information about the RPC server configuration.
 *
 * @see RpcService#servers()
 */
public class RpcServerInfo {
    private final Object rpc;

    private final Set<String> tags;

    private final List<RpcInterfaceInfo<?>> interfaces;

    /**
     * Constructs a new instance.
     *
     * @param rpc See {@link #rpc()}
     * @param interfaces See {@link #interfaces()}
     * @param tags See {@link #tags()}
     */
    public RpcServerInfo(Object rpc, Collection<RpcInterfaceInfo<?>> interfaces, Collection<String> tags) {
        ArgAssert.notNull(rpc, "RPC");

        this.rpc = rpc;
        this.interfaces = unmodifiableList(StreamUtils.nullSafe(interfaces).collect(toList()));
        this.tags = unmodifiableSet(StreamUtils.nullSafe(tags).collect(toSet()));
    }

    /**
     * Returns the RPC server object.
     *
     * @return RPC server object (see {@link RpcServerConfig#setHandler(Object)}).
     */
    public Object rpc() {
        return rpc;
    }

    /**
     * Returns the list of RPC interfaces that are supported by this RPC server.
     *
     * @return List of RPC interfaces that are supported by this RPC server (see {@link Rpc}).
     */
    public List<RpcInterfaceInfo<?>> interfaces() {
        return interfaces;
    }

    /**
     * Returns the set of tags that are configured for this RPC server.
     *
     * @return set of tags that are configured for this RPC server (see {@link RpcServerConfig#setTags(Set)}).
     */
    public Set<String> tags() {
        return tags;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
