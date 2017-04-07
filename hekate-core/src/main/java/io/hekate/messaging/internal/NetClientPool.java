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
import io.hekate.messaging.unicast.SendCallback;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NetClientPool<T> implements ClientPool<T> {
    private static final Logger log = LoggerFactory.getLogger(MessagingGateway.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    private final ClusterNode node;

    private final NetSenderContext<T> client;

    @ToStringIgnore
    private final Object mux = new Object();

    private volatile boolean connected;

    private volatile boolean closed;

    public NetClientPool(String name, ClusterNode node, NetworkConnector<MessagingProtocol> net, MessagingGateway<T> gateway,
        boolean trackIdle) {
        assert name != null : "Name is null.";
        assert node != null : "Cluster node is null.";
        assert net != null : "Network connector is null.";
        assert gateway != null : "Gateway is null.";

        if (DEBUG) {
            log.debug("Creating new connection pool [channel={}, node={}]", name, node);
        }

        this.name = name;
        this.node = node;

        NetworkClient<MessagingProtocol> netClient = net.newClient();

        DefaultMessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(gateway.getId(), gateway.getChannel());

        this.client = new NetSenderContext<>(node.getAddress(), netClient, gateway, endpoint, trackIdle);
    }

    @Override
    public ClusterNode getNode() {
        return node;
    }

    @Override
    public void send(MessageContext<T> ctx, SendCallback callback) {
        // Touch in advance to make sure that pool will not be disconnected
        // by a concurrent thread that executes disconnectIfIdle() method.
        client.touch();

        ensureConnected();

        client.sendNotification(ctx, callback);
    }

    @Override
    public void request(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
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

            return Collections.singletonList(client.disconnect());
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

                    client.disconnect();
                }
            }
        }
    }

    // Package level for testing purposes.
    List<NetSenderContext<T>> getClients() {
        return Collections.singletonList(client);
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

                        client.connect();

                        client.touch();

                        connected = true;
                    }
                }
            }
        }
    }

    private boolean isIdle() {
        return client.isIdle();
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
