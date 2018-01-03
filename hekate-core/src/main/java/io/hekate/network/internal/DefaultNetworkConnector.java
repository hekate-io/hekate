/*
 * Copyright 2018 The Hekate Project
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

package io.hekate.network.internal;

import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.netty.NettyClientFactory;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;

class DefaultNetworkConnector<T> implements NetworkConnector<T> {
    private final String protocol;

    @ToStringIgnore
    private final NettyClientFactory<T> factory;

    public DefaultNetworkConnector(String protocol, NettyClientFactory<T> factory) {
        this.protocol = protocol;
        this.factory = factory;
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public NetworkClient<T> newClient() {
        return factory.newClient();
    }

    @Override
    public String toString() {
        return ToString.format(NetworkConnector.class, this);
    }
}
