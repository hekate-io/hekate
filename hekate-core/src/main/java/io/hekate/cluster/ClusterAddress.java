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

import io.hekate.core.internal.util.ArgAssert;
import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Address of the {@link ClusterNode}.
 *
 * <p>
 * This address includes both the {@link #id() node ID} and the {@link #socket() socket address} that can be used to establish network
 * connections with this node.
 * </p>
 *
 * @see ClusterNode#address()
 */
public class ClusterAddress implements Comparable<ClusterAddress>, Serializable {
    private static final long serialVersionUID = 1;

    private final InetSocketAddress socket;

    private final ClusterNodeId id;

    /**
     * Constructs new instance with the specified network address and node identifier.
     *
     * @param socket Network address of {@link ClusterNode}.
     * @param id Unique identifier of {@link ClusterNode}.
     */
    public ClusterAddress(InetSocketAddress socket, ClusterNodeId id) {
        ArgAssert.notNull(socket, "Socket address");
        ArgAssert.notNull(id, "ID");

        this.socket = socket;
        this.id = id;
    }

    /**
     * Returns the unique identifier of the {@link ClusterNode}.
     *
     * @return Unique identifier of the {@link ClusterNode}.
     */
    public ClusterNodeId id() {
        return id;
    }

    /**
     * Returns the network address of the {@link ClusterNode}.
     *
     * @return Network address of the {@link ClusterNode}.
     */
    public InetSocketAddress socket() {
        return socket;
    }

    /**
     * Returns the host address.
     *
     * @return Host address.
     */
    public String host() {
        return socket.getAddress().getHostAddress();
    }

    /**
     * Returns the port number.
     *
     * @return Port number.
     */
    public int port() {
        return socket.getPort();
    }

    /**
     * Compares this address with the specified one by comparing {@link #id()}s of both addresses.
     */
    @Override
    public int compareTo(ClusterAddress o) {
        return id.compareTo(o.id);
    }

    /**
     * Returns {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #id()} as this object.
     *
     * @param o Object.
     *
     * @return {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #id()} as this object.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof ClusterAddress)) {
            return false;
        }

        ClusterAddress that = (ClusterAddress)o;

        return id.equals(that.id);
    }

    /**
     * Returns the hash code of {@link #id()}.
     */
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    /**
     * Returns the string representation of this address formatted as {@link #socket()}:{@link #id()}.
     *
     * @return String representation of this address formatted as {@link #socket()}:{@link #id()}.
     */
    @Override
    public String toString() {
        return socket + ":" + id;
    }
}
