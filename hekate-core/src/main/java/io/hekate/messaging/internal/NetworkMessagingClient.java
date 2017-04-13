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

class NetworkMessagingClient<T> implements MessagingClient<T> {
    private static final Logger log = LoggerFactory.getLogger(MessagingGateway.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private final String name;

    private final ClusterNode node;

    private final NetworkOutboundConnection<T> conn;

    private final boolean trackIdle;

    @ToStringIgnore
    private final Object mux = new Object();

    @ToStringIgnore
    private volatile boolean idle;

    private volatile boolean connected;

    private volatile boolean closed;

    public NetworkMessagingClient(String name, ClusterNode node, NetworkConnector<MessagingProtocol> net, MessagingGateway<T> gateway,
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
        this.trackIdle = trackIdle;

        NetworkClient<MessagingProtocol> netClient = net.newClient();

        DefaultMessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(gateway.getId(), gateway.getChannel());

        this.conn = new NetworkOutboundConnection<>(node.getAddress(), netClient, gateway, endpoint);
    }

    @Override
    public ClusterNode getNode() {
        return node;
    }

    @Override
    public void send(MessageContext<T> ctx, SendCallback callback) {
        touch();

        ensureConnected();

        conn.sendNotification(ctx, callback);
    }

    @Override
    public void streamRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        touch();

        ensureConnected();

        conn.sendStreamRequest(ctx, callback);
    }

    @Override
    public void singleRequest(MessageContext<T> ctx, InternalRequestCallback<T> callback) {
        touch();

        ensureConnected();

        conn.sendSingleRequest(ctx, callback);
    }

    @Override
    public List<NetworkFuture<MessagingProtocol>> close() {
        if (DEBUG) {
            log.debug("Closing connection [channel={}, node={}]", name, node);
        }

        synchronized (mux) {
            // Mark as closed.
            closed = true;

            return Collections.singletonList(conn.disconnect());
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
        if (trackIdle) {
            if (connected) {
                if (idle && !conn.hasPendingRequests()) {
                    synchronized (mux) {
                        // Double check with lock.
                        if (connected && idle && !conn.hasPendingRequests()) {
                            if (DEBUG) {
                                log.debug("Disconnecting idle connection [chanel={}, node={}]", name, node);
                            }

                            connected = false;

                            conn.disconnect();
                        }
                    }
                }

                idle = true;
            }
        }
    }

    @Override
    public void touch() {
        if (trackIdle) {
            if (idle) {
                synchronized (mux) {
                    if (!closed && connected) {
                        idle = false;
                    }
                }
            }
        }
    }

    // Package level for testing purposes.
    NetworkOutboundConnection<T> getConnection() {
        return conn;
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

                        conn.connect();

                        connected = true;
                        idle = false;
                    }
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}