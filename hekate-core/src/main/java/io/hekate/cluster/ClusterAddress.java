/*
 * Copyright 2017 The Hekate Project
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

import java.io.Serializable;
import java.net.InetSocketAddress;

/**
 * Address of a {@link ClusterNode}.
 *
 * <p>
 * This address includes both the {@link #getId() node ID} and the {@link #getSocket() socket address} that can be used to establish network
 * connections with this node.
 * </p>
 *
 * @see ClusterNode#getAddress()
 */
public class ClusterAddress implements Comparable<ClusterAddress>, Serializable {
    private static final long serialVersionUID = 1;

    private final InetSocketAddress socket;

    private final ClusterUuid id;

    /**
     * Constructs new instance with the specified network address and node identifier.
     *
     * @param socket Network address of {@link ClusterNode}.
     * @param id Unique identifier of {@link ClusterNode}.
     */
    public ClusterAddress(InetSocketAddress socket, ClusterUuid id) {
        assert socket != null : "Address is null.";
        assert id != null : "ID is null.";

        this.socket = socket;
        this.id = id;
    }

    /**
     * Returns unique identifier of {@link ClusterNode}.
     *
     * @return Unique identifier of {@link ClusterNode}.
     */
    public ClusterUuid getId() {
        return id;
    }

    /**
     * Returns network address of {@link ClusterNode}.
     *
     * @return Network address of {@link ClusterNode}.
     */
    public InetSocketAddress getSocket() {
        return socket;
    }

    /**
     * Compares this address with the specified one by comparing {@link #getId() node idnetifiers} of both addresses.
     */
    @Override
    public int compareTo(ClusterAddress o) {
        return id.compareTo(o.id);
    }

    /**
     * Returns {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #getId() identifier} as this
     * object.
     *
     * @param o Object.
     *
     * @return {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #getId() identifier} as this
     * object.
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
     * Returns the hash code of the {@link #getId() node identifier}.
     */
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return socket + ":" + id;
    }
}
