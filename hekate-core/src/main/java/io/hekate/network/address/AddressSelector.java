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

package io.hekate.network.address;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.Hekate;
import io.hekate.core.HekateException;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetAddress;

/**
 * Network address selector.
 *
 * <p>
 * Implementation of this interface is used by {@link Hekate} to resolve which of its local IP addresses should be advertised to remote
 * nodes so that they could connect to the local node. The resolved address becomes part of {@link ClusterNode#address()}.
 * </p>
 *
 * <p>
 * Implementations of this interface can be registered via {@link NetworkServiceFactory#setAddressSelector(AddressSelector)}.
 * </p>
 *
 * <p>
 * Default implementation of this interface is {@link DefaultAddressSelector}. It provides support for configurable filtering of local
 * network interfaces during address resolution. Please see its documentation for more details about its capabilities and configuration
 * options.
 * </p>
 *
 * @see NetworkServiceFactory#setHost(String)
 * @see NetworkServiceFactory#setAddressSelector(AddressSelector)
 * @see ClusterNode#address()
 */
public interface AddressSelector {
    /**
     * Resolves the IP address for the local node. The returned address will be used by remote nodes to connect to the local node.
     *
     * @param bindAddress Bind address of the node's TCP acceptor server. This address is based on a value of the {@link
     * NetworkServiceFactory#setHost(String)} parameter and can be a wildcard address.
     *
     * @return Local node address.
     *
     * @throws HekateException If address resolution fails.
     */
    InetAddress select(InetAddress bindAddress) throws HekateException;
}
