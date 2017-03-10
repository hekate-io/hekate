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
 * This address includes both {@link ClusterNodeId} of the node and its {@link InetSocketAddress} that can be used to establish TCP network
 * connections.
 * </p>
 *
 * <p>
 * <b>Note:</b> all {@link #equals(Object) equality} and {@link #compareTo(ClusterAddress) comparison} operations are based on {@link
 * ClusterNodeId} value. In order to check for network address equality please consider using {@link #isSameNetAddress(ClusterAddress)}
 * method.
 * </p>
 *
 * @see ClusterNode#getAddress()
 */
public class ClusterAddress implements Comparable<ClusterAddress>, Serializable {
    private static final long serialVersionUID = 1;

    private final InetSocketAddress netAddress;

    private final ClusterNodeId id;

    /**
     * Constructs new instance with the specified network address and node identifier.
     *
     * @param netAddress Network address of {@link ClusterNode}.
     * @param id Unique identifier of {@link ClusterNode}.
     */
    public ClusterAddress(InetSocketAddress netAddress, ClusterNodeId id) {
        assert netAddress != null : "Address is null.";
        assert id != null : "ID is null.";

        this.netAddress = netAddress;
        this.id = id;
    }

    /**
     * Returns unique identifier of {@link ClusterNode}.
     *
     * @return Unique identifier of {@link ClusterNode}.
     */
    public ClusterNodeId getId() {
        return id;
    }

    /**
     * Returns network address of {@link ClusterNode}.
     *
     * @return Network address of {@link ClusterNode}.
     */
    public InetSocketAddress getNetAddress() {
        return netAddress;
    }

    /**
     * Returns {@code true} if this instance has the same {@link #getNetAddress() network address} with the specified one.
     *
     * @param other Other address.
     *
     * @return {@code true} if this instance has the same {@link #getNetAddress() network address} with the specified one.
     */
    public boolean isSameNetAddress(ClusterAddress other) {
        return netAddress.equals(other.getNetAddress());
    }

    /**
     * Compares this address with the specified one by comparing {@link #getId() node idnetifiers} of both addresses.
     */
    @Override
    public int compareTo(ClusterAddress o) {
        return id.compareTo(o.id);
    }

    /**
     * Returns {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #getId() node identifier}.
     *
     * @param o Object.
     *
     * @return {@code true} if the specified object is of {@link ClusterAddress} type and has the same {@link #getId() node identifier}.
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
     * Returns the hash code of {@link #getId() node identifier}.
     */
    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return netAddress + ":" + id;
    }
}
