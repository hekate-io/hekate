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
import io.hekate.messaging.MessagingChannelId;
import io.hekate.messaging.MessagingEndpoint;
import io.hekate.messaging.internal.MessagingProtocol.Connect;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;

class NetSenderContext<T> extends NetReceiverContextBase<T> {
    private final ClusterAddress address;

    private final NetworkClient<MessagingProtocol> net;

    private final MessagingChannelId channelId;

    private final Object mux = new Object();

    private int epoch;

    public NetSenderContext(ClusterAddress address, NetworkClient<MessagingProtocol> net, MessagingGateway<T> gateway,
        MessagingEndpoint<T> endpoint, boolean trackIdleState) {
        super(net, gateway, endpoint, trackIdleState);

        assert address != null : "Address is null.";

        this.channelId = gateway.getId();
        this.address = address;
        this.net = net;
    }

    public NetworkFuture<MessagingProtocol> connect() {
        synchronized (mux) {
            touch();

            // Update connection epoch.
            int localEpoch = ++epoch;

            setEpoch(localEpoch);

            final InetSocketAddress netAddress = address.getSocket();

            int poolOrder = getEndpoint().getSocketOrder();
            int poolSize = getEndpoint().getSockets();

            Connect connectMsg = new Connect(address.getId(), channelId, poolOrder, poolSize);

            return net.connect(netAddress, connectMsg, new NetworkClientCallback<MessagingProtocol>() {
                @Override
                public void onMessage(NetworkMessage<MessagingProtocol> message, NetworkClient<MessagingProtocol> from) throws IOException {
                    receive(message, from);
                }

                @Override
                public void onFailure(NetworkClient<MessagingProtocol> client, Throwable error) {
                    synchronized (mux) {
                        // Check if there were no disconnects for this epoch.
                        if (localEpoch == epoch) {
                            Connect msg = new Connect(address.getId(), channelId, poolOrder, poolSize);

                            client.ensureConnected(netAddress, msg, this);
                        }
                    }

                    discardRequests(localEpoch, error);
                }

                @Override
                public void onDisconnect(NetworkClient<MessagingProtocol> client) {
                    synchronized (mux) {
                        // Check if there were no disconnects for this epoch.
                        if (localEpoch == epoch) {
                            Connect msg = new Connect(address.getId(), channelId, poolOrder, poolSize);

                            client.ensureConnected(netAddress, msg, this);
                        }
                    }

                    discardRequests(localEpoch, new ClosedChannelException());
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

    // Package level for testing purposes.
    NetworkClient.State getNetState() {
        return net.getState();
    }
}
