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
import io.hekate.util.format.ToStringIgnore;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.stream.Collectors.toList;

/**
 * Meta-information about an {@link Rpc}-annotated interface.
 *
 * @param <T> Interface type.
 *
 * @see RpcServerInfo#interfaces()
 */
public class RpcInterfaceInfo<T> {
    private final Class<T> javaType;

    private final int version;

    private final int minClientVersion;

    @ToStringIgnore
    private final String versionedName;

    @ToStringIgnore
    private final List<RpcMethodInfo> methods;

    /**
     * Constructs a new instance.
     *
     * @param javaType See {@link #javaType()}.
     * @param version See {@link #version()}
     * @param minClientVersion See {@link #minClientVersion()}.
     * @param methods See {@link #methods()}.
     */
    public RpcInterfaceInfo(Class<T> javaType, int version, int minClientVersion, List<RpcMethodInfo> methods) {
        ArgAssert.notNull(javaType, "Type");
        ArgAssert.notNull(methods, "Methods list");

        this.javaType = javaType;
        this.version = version;
        this.minClientVersion = minClientVersion;
        this.versionedName = javaType.getName() + ':' + version;
        this.methods = unmodifiableList(StreamUtils.nullSafe(methods).collect(toList()));
    }

    /**
     * Returns the RPC interface name.
     *
     * <p>
     * This method is merely a shortcut for {@link #javaType()}{@code .getName()}.
     * </p>
     *
     * @return RPC interface name.
     */
    public String name() {
        return javaType.getName();
    }

    /**
     * Returns the versioned name of this RPC interface.
     *
     * <p>
     * Versioned name is a combination of {@link #name()} and {@link #version()}.
     * </p>
     *
     * @return Versioned name.
     */
    public String versionedName() {
        return versionedName;
    }

    /**
     * Returns the Java type of this RPC interface.
     *
     * @return Java type of this RPC interface.
     */
    public Class<T> javaType() {
        return javaType;
    }

    /**
     * Returns the version of this RPC interface.
     *
     * @return Version of this RPC interface.
     *
     * @see Rpc#version()
     */
    public int version() {
        return version;
    }

    /**
     * Returns the minimum client version that is supported by this RPC interface.
     *
     * @return Minimum client version that is supported by this RPC interface.
     *
     * @see Rpc#minClientVersion()
     */
    public int minClientVersion() {
        return minClientVersion;
    }

    /**
     * Returns the meta-information about this RPC interface's methods.
     *
     * @return meta-information about this RPC interface's methods.
     */
    public List<RpcMethodInfo> methods() {
        return methods;
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
