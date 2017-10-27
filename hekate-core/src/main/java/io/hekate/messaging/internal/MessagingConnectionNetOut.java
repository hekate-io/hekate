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

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.MessagingException;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;

class MessagingConnectionNetOut<T> extends MessagingConnectionNetBase<T> {
    private final ClusterAddress address;

    private final NetworkClient<MessagingProtocol> net;

    private final MessagingChannelId channelId;

    private final ClusterNodeId localNodeId;

    private final Object mux = new Object();

    private int epoch;

    public MessagingConnectionNetOut(ClusterAddress address, NetworkClient<MessagingProtocol> net, MessagingGateway<T> gateway,
        MessagingEndpoint<T> endpoint) {
        super(net, gateway, endpoint);

        assert address != null : "Address is null.";

        this.channelId = gateway.id();
        this.localNodeId = gateway.localNode().id();
        this.address = address;
        this.net = net;
    }

    public NetworkFuture<MessagingProtocol> connect() {
        synchronized (mux) {
            // Update connection epoch.
            int localEpoch = ++epoch;

            setEpoch(localEpoch);

            final InetSocketAddress netAddress = address.socket();

            Connect connectMsg = new Connect(address.id(), localNodeId, channelId);

            return net.connect(netAddress, connectMsg, new NetworkClientCallback<MessagingProtocol>() {
                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> message, NetworkClient<MessagingProtocol> from) throws IOException {
                    receive(message, from);
                }

                @Override
                public void onDisconnect(NetworkClient<MessagingProtocol> client, Optional<Throwable> cause) {
                    synchronized (mux) {
                        // Check if there were no disconnects for this epoch.
                        if (localEpoch == epoch) {
                            Connect msg = new Connect(address.id(), localNodeId, channelId);

                            client.ensureConnected(netAddress, msg, this);
                        }
                    }

                    discardRequests(localEpoch, new MessagingException("Messaging operation failed [remote-node-id=" + address.id()
                        + ", address=" + netAddress + ']', cause.orElseGet(ClosedChannelException::new)));
                }
            });
        }
    }

    @Override
    public NetworkFuture<MessagingProtocol> disconnect() {
        synchronized (mux) {
            // Prevent auto-reconnect.
            epoch++;

            return super.disconnect();
        }
    }
}
