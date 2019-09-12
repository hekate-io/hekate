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

package io.hekate.network.address;

import io.hekate.cluster.ClusterNode;
import io.hekate.core.HekateException;
import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import java.net.InetAddress;

/**
 * Network host address selector.
 *
 * <p>
 * Implementation of this interface is used by {@link NetworkService} to resolve which of its local IP addresses should be advertised to
 * remote nodes so that they could connect to the local node. The resolved address becomes a part of the {@link ClusterNode#address()}.
 * </p>
 *
 * <p>
 * Implementations of this interface can be registered via {@link NetworkServiceFactory#setHostSelector(AddressSelector)}.
 * </p>
 *
 * <p>
 * Default implementation of this interface is the {@link AddressPattern} class. It provides support for configurable pattern-based
 * filtering of the network interfaces during address resolution. Please see its documentation for more details about its capabilities and
 * configuration options.
 * </p>
 *
 * @see NetworkServiceFactory#setHostSelector(AddressSelector)
 * @see ClusterNode#address()
 */
public interface AddressSelector {
    /**
     * Resolves the IP address for the local node. The returned address will be used by remote nodes to connect to the local node.
     *
     * @return Local node address.
     *
     * @throws HekateException If address resolution fails.
     */
    InetAddress select() throws HekateException;
}
