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

package io.hekate.messaging.internal;

import io.hekate.messaging.MessagingEndpoint;
import io.hekate.network.NetworkEndpoint;
import java.nio.channels.ClosedChannelException;

class NetworkInboundConnection<T> extends NetworkConnectionBase<T> {
    public NetworkInboundConnection(NetworkEndpoint<MessagingProtocol> net, MessagingEndpoint<T> endpoint, MessagingGateway<T> gateway) {
        super(net, gateway, endpoint);
    }

    public void onConnect() {
        gateway().getReceiver().onConnect(getEndpoint());
    }

    public void onDisconnect() {
        gateway().getReceiver().onDisconnect(getEndpoint());

        discardRequests(new ClosedChannelException());

        gateway().unregister(this);
    }
}