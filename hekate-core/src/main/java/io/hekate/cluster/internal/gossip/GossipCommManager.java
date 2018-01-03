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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.gossip.GossipProtocol.Connect;
import io.hekate.cluster.internal.gossip.GossipProtocol.GossipMessage;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GossipCommManager implements NetworkServerHandler<GossipProtocol> {
    public interface Callback {
        void onReceive(GossipProtocol msg);

        void onSendSuccess(GossipProtocol msg);

        void onSendFailure(GossipProtocol msg, Throwable error);
    }

    private static class EndpointHolder {
        private final boolean outbound;

        private final NetworkEndpoint<GossipProtocol> endpoint;

        public EndpointHolder(NetworkEndpoint<GossipProtocol> endpoint, boolean outbound) {
            this.endpoint = endpoint;
            this.outbound = outbound;
        }

        public NetworkEndpoint<GossipProtocol> endpoint() {
            return endpoint;
        }

        public boolean isOutbound() {
            return outbound;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(GossipCommManager.class);

    private static final boolean DEBUG = log.isDebugEnabled();

    private static final boolean TRACE = log.isDebugEnabled();

    private final Object mux = new Object();

    private final Map<ClusterNodeId, EndpointHolder> clients = new HashMap<>();

    private final Callback callback;

    private final NetworkConnector<GossipProtocol> net;

    private final NetworkClientCallback<GossipProtocol> netClientCallback;

    public GossipCommManager(NetworkConnector<GossipProtocol> net, Callback callback) {
        assert net != null : "Network connector is null.";
        assert callback != null : "Callback is null.";

        this.net = net;
        this.callback = callback;

        this.netClientCallback = new NetworkClientCallback<GossipProtocol>() {
            @Override
            public void onMessage(NetworkMessage<GossipProtocol> packet, NetworkClient<GossipProtocol> client) throws IOException {
                GossipProtocol msg = packet.decode();

                if (TRACE) {
                    log.trace("Received message via outbound connection [message={}]", msg);
                }

                callback.onReceive(msg);
            }

            @Override
            public void onDisconnect(NetworkClient<GossipProtocol> client, Optional<Throwable> cause) {
                ClusterNodeId id = client.getContext();

                if (id != null) {
                    if (DEBUG) {
                        log.debug("Closed outbound connection [to={}]", id);
                    }

                    synchronized (mux) {
                        EndpointHolder latestSender = clients.get(id);

                        // Remove from map only if it contains exactly the same client instance.
                        if (latestSender != null && latestSender.endpoint() == client) {
                            if (DEBUG) {
                                log.debug("Removing outbound connection from registry [to={}]", id);
                            }

                            clients.remove(id);
                        }
                    }
                }
            }
        };
    }

    public void send(GossipMessage msg, Runnable onComplete) {
        ClusterAddress addr = msg.to();

        ClusterNodeId id = addr.id();

        Callback localCallback;

        EndpointHolder holder;

        boolean newConnection = false;

        synchronized (mux) {
            localCallback = callback;

            holder = clients.get(id);

            if (holder == null) {
                newConnection = true;

                NetworkClient<GossipProtocol> client = net.newClient();

                client.setContext(id);

                holder = new EndpointHolder(client, true);

                clients.put(id, holder);

                Connect connectMsg = new Connect(msg.from().id());

                client.connect(addr.socket(), connectMsg, netClientCallback);
            }
        }

        if (newConnection && DEBUG) {
            log.debug("Created a new outbound connection [to={}]", addr);
        }

        if (TRACE) {
            log.trace("Sending message [outboundConnection={}, message={}]", holder.isOutbound(), msg);
        }

        holder.endpoint().send(msg, (sent, error, endpoint) -> {
            if (error.isPresent()) {
                if (TRACE) {
                    log.trace("Failed to send a message [reason={}, message={}]", error.get(), sent);
                }

                localCallback.onSendFailure(sent, error.get());

                if (onComplete != null) {
                    onComplete.run();
                }
            } else {
                localCallback.onSendSuccess(sent);

                if (onComplete != null) {
                    onComplete.run();
                }
            }
        });
    }

    public void sendAndDisconnect(GossipProtocol msg, Runnable onComplete) {
        NetworkClient<GossipProtocol> sendOnceClient = net.newClient();

        sendOnceClient.connect(msg.toAddress(), msg, new NetworkClientCallback<GossipProtocol>() {
            @Override
            public void onMessage(NetworkMessage<GossipProtocol> message, NetworkClient<GossipProtocol> from) {
                // No-op.
            }

            @Override
            public void onConnect(NetworkClient<GossipProtocol> client) {
                client.disconnect();

                callback.onSendSuccess(msg);

                if (onComplete != null) {
                    onComplete.run();
                }
            }

            @Override
            public void onDisconnect(NetworkClient<GossipProtocol> client, Optional<Throwable> cause) {
                cause.ifPresent(err -> {
                    callback.onSendFailure(msg, err);

                    if (onComplete != null) {
                        onComplete.run();
                    }
                });
            }
        });
    }

    @Override
    public void onConnect(GossipProtocol msg, NetworkEndpoint<GossipProtocol> client) {
        if (msg.type() == GossipProtocol.Type.CONNECT) {
            Connect connect = (Connect)msg;

            ClusterNodeId id = connect.nodeId();

            if (id == null) {
                if (DEBUG) {
                    log.debug("Rejecting connection without a cluster node id [message={}, connection={}]", msg, client);
                }

                client.disconnect();
            } else {
                if (DEBUG) {
                    log.debug("Got a new inbound connection [from={}]", id);
                }

                client.setContext(id);

                synchronized (mux) {
                    if (clients.containsKey(id)) {
                        if (DEBUG) {
                            log.debug("Will not register inbound connection since another connection exists [from={}]", id);
                        }
                    } else {
                        if (DEBUG) {
                            log.debug("Registering inbound connection [from={}]", id);
                        }

                        clients.put(id, new EndpointHolder(client, false));
                    }
                }
            }
        } else {
            client.disconnect();

            callback.onReceive(msg);
        }
    }

    @Override
    public void onMessage(NetworkMessage<GossipProtocol> packet, NetworkEndpoint<GossipProtocol> from) throws IOException {
        GossipProtocol msg = packet.decode();

        if (TRACE) {
            log.trace("Received message via inbound connection [message={}]", msg);
        }

        callback.onReceive(msg);
    }

    @Override
    public void onDisconnect(NetworkEndpoint<GossipProtocol> client) {
        ClusterNodeId id = client.getContext();

        if (id != null) {
            if (DEBUG) {
                log.debug("Closed inbound connection [from={}]", id);
            }

            synchronized (mux) {
                EndpointHolder holder = clients.get(id);

                // Remove from map only if it contains exactly the same client instance.
                if (holder != null && holder.endpoint() == client) {
                    if (DEBUG) {
                        log.debug("Removing inbound connection from registry [from={}]", id);
                    }

                    clients.remove(id);
                }
            }
        }
    }

    public void stop() {
        List<NetworkFuture<GossipProtocol>> clientsFuture;

        synchronized (mux) {
            clientsFuture = new ArrayList<>();

            // Need to create a local clients list to prevent concurrent modification errors
            // since each client removes itself from this map.
            List<EndpointHolder> localClients = new ArrayList<>(clients.values());

            clients.clear();

            if (!localClients.isEmpty()) {
                if (DEBUG) {
                    log.debug("Closing connections  [size={}]", localClients.size());
                }

                // Disconnect clients.
                localClients.stream()
                    .filter(EndpointHolder::isOutbound)
                    .forEach(c ->
                        clientsFuture.add(c.endpoint().disconnect())
                    );
            }
        }

        // Await for clients termination.
        for (NetworkFuture<GossipProtocol> future : clientsFuture) {
            try {
                AsyncUtils.getUninterruptedly(future);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException) {
                    if (DEBUG) {
                        log.debug("Failed to close network connection due to an I/O error [cause={}]", e.toString());
                    }
                } else {
                    log.warn("Failed to close network connection.", cause);
                }
            }
        }
    }
}
