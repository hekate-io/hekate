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

package io.hekate.messaging.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkFuture;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MessagingClient<T> {
    private static final Logger log = LoggerFactory.getLogger(MessagingClient.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final int STATE_DISCONNECTED = 1;

    private static final int STATE_CONNECTED = 2;

    private static final int STATE_IDLE = 3;

    private static final int STATE_CLOSED = 4;

    private final MessagingGatewayContext<T> ctx;

    private final MessagingConnectionOut<T> conn;

    private final ClusterNode remoteNode;

    private final boolean trackIdle;

    @ToStringIgnore
    private final Object mux = new Object();

    private volatile int state = STATE_DISCONNECTED;

    public MessagingClient(
        ClusterNode remoteNode,
        NetworkConnector<MessagingProtocol> net,
        MessagingGatewayContext<T> ctx,
        boolean trackIdle
    ) {
        assert remoteNode != null : "Remote node is null.";
        assert net != null : "Network connector is null.";
        assert ctx != null : "Messaging context is null.";

        if (DEBUG) {
            log.debug("Creating new connection [channel={}, node={}]", ctx.channel().name(), remoteNode);
        }

        this.ctx = ctx;
        this.remoteNode = remoteNode;
        this.trackIdle = trackIdle;

        NetworkClient<MessagingProtocol> client = net.newClient();

        DefaultMessagingEndpoint<T> endpoint = new DefaultMessagingEndpoint<>(remoteNode.address(), ctx.channel());

        this.conn = new MessagingConnectionOut<>(client, ctx, endpoint, mux, () -> {
            // On internal disconnect:
            synchronized (mux) {
                if (state != STATE_CLOSED) {
                    state = STATE_DISCONNECTED;
                }
            }
        });
    }

    public ClusterNode node() {
        return remoteNode;
    }

    public MessagingConnectionOut<T> connection() {
        ensureConnected();

        return conn;
    }

    public List<NetworkFuture<MessagingProtocol>> close() {
        if (DEBUG) {
            log.debug("Closing connection [channel={}, node={}]", ctx.channel().name(), remoteNode);
        }

        synchronized (mux) {
            // Mark as closed.
            state = STATE_CLOSED;

            return Collections.singletonList(conn.disconnect());
        }
    }

    public boolean isConnected() {
        synchronized (mux) {
            return state == STATE_CONNECTED || state == STATE_IDLE;
        }
    }

    public void disconnectIfIdle() {
        if (trackIdle) {
            if (state == STATE_CONNECTED) {
                if (conn.state() == NetworkClient.State.CONNECTED && !conn.hasPendingRequests()) {
                    synchronized (mux) {
                        // Double check with lock.
                        if (state == STATE_CONNECTED) {
                            if (conn.state() == NetworkClient.State.CONNECTED && !conn.hasPendingRequests()) {
                                state = STATE_IDLE;
                            }
                        }
                    }
                }
            } else if (state == STATE_IDLE) {
                synchronized (mux) {
                    // Double check with lock.
                    if (state == STATE_IDLE) {
                        if (conn.hasPendingRequests()) {
                            state = STATE_CONNECTED;
                        } else {
                            if (DEBUG) {
                                log.debug("Disconnecting idle connection [chanel={}, node={}]", ctx.channel().name(), remoteNode);
                            }

                            state = STATE_DISCONNECTED;

                            conn.disconnect();
                        }
                    }
                }
            }
        }
    }

    public void touch() {
        if (trackIdle && state == STATE_IDLE) {
            synchronized (mux) {
                if (state == STATE_IDLE) {
                    state = STATE_CONNECTED;
                }
            }
        }
    }

    private void ensureConnected() {
        if (state != STATE_CONNECTED) {
            synchronized (mux) {
                // Double check with lock.
                if (state == STATE_DISCONNECTED) {
                    if (DEBUG) {
                        log.debug("Initializing connection [chanel={}, node={}]", ctx.channel().name(), remoteNode);
                    }

                    // Important to connect before updating the 'state' flag.
                    // Otherwise the double check logic will be broken and concurrent threads
                    // will try to access the client while it is not connected yet.
                    conn.connect();

                    state = STATE_CONNECTED;
                } else if (state == STATE_IDLE) {
                    state = STATE_CONNECTED;
                }
            }
        }
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
