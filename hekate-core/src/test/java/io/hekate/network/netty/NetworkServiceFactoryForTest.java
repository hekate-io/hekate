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

package io.hekate.network.netty;

import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;
import io.hekate.network.internal.NettyNetworkService;

public class NetworkServiceFactoryForTest extends NetworkServiceFactory {
    private NettySpyForTest spy;

    public NettySpyForTest getSpy() {
        return spy;
    }

    public void setSpy(NettySpyForTest spy) {
        this.spy = spy;
    }

    @Override
    public NetworkService createService() {
        return new NettyNetworkService(this) {
            @Override
            protected <T> NettyClientFactory<T> createClientFactory() {
                NettyClientFactory<T> factory = super.createClientFactory();

                factory.setSpy(spy);

                return factory;
            }
        };
    }
}
