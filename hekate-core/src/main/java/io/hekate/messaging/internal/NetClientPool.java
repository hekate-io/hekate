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

import io.hekate.cluster.ClusterNode;
import io.hekate.core.internal.util.Utils;
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NetClientPool<T> implements ClientPool<T> {
    private static final Logger log = LoggerFactory.getLogger(MessagingGateway.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    private final ClusterNode node;

    private final NetSenderContext<T>[] clients;

    @ToStringIgnore
    private final NetSenderContext<T> single;

    @ToStringIgnore
    private final Object mux = new Object();

    private volatile boolean connected;

    private volatile boolean closed;

    public NetClientPool(String name, ClusterNode node, NetworkConnector<MessagingProtocol> net, int poolSize, MessagingGateway<T> gateway,
        boolean trackIdle) {
        assert name != null : "Name is null.";
        assert node != null : "Cluster node is null.";
        assert net != null : "Network connector is null.";
        assert gateway != null : "Gateway is null.";
        assert poolSize > 0 : "Size must be above zero.";

        if (DEBUG) {
            log.debug("Creating new connection pool [channel={}, node={}, size={}]", name, node, poolSize);
        }

        this.name = name;
        this.node = node;

        @SuppressWarnings("unchecked")
        NetSenderContext<T>[] clients = new NetSenderContext[poolSize];

        for (int i = 0; i < clients.length; i++) {
            NetworkClient<MessagingProtocol> client = net.newClient();

            DefaultMessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(gateway.getId(), i, poolSize, gateway.getChannel());

            NetSenderContext<T> commClient = new NetSenderContext<>(node.getAddress(), client, gateway, endpoint, trackIdle);

            clients[i] = commClient;
        }

        this.clients = clients;

        single = poolSize == 1 ? clients[0] : null;
    }

    @Override
    public ClusterNode getNode() {
        return node;
    }

    @Override
    public void send(MessageContext<T> ctx, SendCallback callback) {
        NetSenderContext<T> client = route(ctx.getAffinity());

        // Touch in advance to make sure that pool will not be disconnected
        // by a concurrent thread that executes disconnectIfIdle() method.
        client.touch();

        ensureConnected();

        client.sendNotification(ctx, callback);
    }

    @Override
    public void request(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        NetSenderContext<T> client = route(ctx.getAffinity());

        // Touch in advance to make sure that pool will not be disconnected
        // by a concurrent thread that executes disconnectIfIdle() method.
        client.touch();

        ensureConnected();

        client.sendRequest(ctx, callback);
    }

    @Override
    public List<NetworkFuture<MessagingProtocol>> close() {
        if (DEBUG) {
            log.debug("Closing connection pool [channel={}, node={}]", name, node);
        }

        synchronized (mux) {
            // Mark as closed.
            closed = true;

            // Disconnect clients and collect their disconnects future.
            List<NetworkFuture<MessagingProtocol>> clientsFuture = new ArrayList<>(clients.length);

            for (NetSenderContext<T> client : clients) {
                NetworkFuture<MessagingProtocol> netFuture = client.disconnect();

                clientsFuture.add(netFuture);
            }

            return clientsFuture;
        }
    }

    @Override
    public boolean isConnected() {
        synchronized (mux) {
            return connected;
        }
    }

    @Override
    public void disconnectIfIdle() {
        if (connected && isIdle()) {
            synchronized (mux) {
                // Double check with lock.
                if (connected && isIdle()) {
                    if (DEBUG) {
                        log.debug("Disconnecting idle connections pool [chanel={}, node={}]", name, node);
                    }

                    connected = false;

                    for (NetSenderContext<T> client : clients) {
                        client.disconnect();
                    }
                }
            }
        }
    }

    // Package level for testing purposes.
    List<NetSenderContext<T>> getClients() {
        return Arrays.asList(clients);
    }

    private void ensureConnected() {
        if (!connected) {
            if (!closed) {
                synchronized (mux) {
                    // Double check with lock.
                    if (!connected && !closed) {
                        if (DEBUG) {
                            log.debug("Initializing connections pool [chanel={}, node={}]", name, node);
                        }

                        for (NetSenderContext<T> client : clients) {
                            client.connect();
                        }

                        for (NetSenderContext<T> client : clients) {
                            client.touch();
                        }

                        connected = true;
                    }
                }
            }
        }
    }

    private boolean isIdle() {
        for (NetSenderContext<T> client : clients) {
            if (!client.isIdle()) {
                return false;
            }
        }

        return true;
    }

    private NetSenderContext<T> route(int affinity) {
        if (single == null) {
            return clients[Utils.mod(affinity, clients.length)];
        } else {
            return single;
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
