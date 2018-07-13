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
import io.hekate.core.internal.util.ErrorUtils;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkConnector;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkMessage;
import io.hekate.network.NetworkServerHandler;
import io.hekate.util.async.AsyncUtils;
import io.hekate.util.format.ToString;
import io.netty.channel.ConnectTimeoutException;
import java.io.IOException;
import java.net.ConnectException;
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

        void onConnectFailure(ClusterAddress address);

        Optional<Throwable> onBeforeSend(GossipProtocol msg);
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
                ClusterAddress address = client.getContext();

                if (address != null) {
                    if (DEBUG) {
                        log.debug("Closed outbound connection [to={}]", address);
                    }

                    boolean unregistered = false;

                    synchronized (mux) {
                        EndpointHolder latestSender = clients.get(address.id());

                        // Remove from map only if it contains exactly the same client instance.
                        if (latestSender != null && latestSender.endpoint() == client) {
                            if (DEBUG) {
                                log.debug("Removing outbound connection from registry [to={}]", address);
                            }

                            clients.remove(address.id());

                            unregistered = true;
                        }
                    }

                    if (unregistered && cause.isPresent()) {
                        // Notify on connect error only if we were not able to connect to the remote node's process.
                        // In case of timeout we can't decide on whether the remote process is alive or not (could be paused by GC, etc).
                        ConnectException connErr = ErrorUtils.findCause(ConnectException.class, cause.get());

                        if (connErr != null && !(connErr instanceof ConnectTimeoutException)) {
                            callback.onConnectFailure(address);
                        }
                    }
                }
            }
        };
    }

    public void send(GossipMessage msg, Runnable onComplete) {
        Optional<Throwable> callbackErr = callback.onBeforeSend(msg);

        if (callbackErr.isPresent()) {
            Throwable err = callbackErr.get();

            if (TRACE) {
                log.trace("Failed to send a message [reason={}, message={}]", err, msg);
            }

            callback.onSendFailure(msg, err);
        } else {
            EndpointHolder holder;

            synchronized (mux) {
                ClusterNodeId nodeId = msg.to().id();

                holder = clients.get(nodeId);

                if (holder == null) {
                    if (DEBUG) {
                        log.debug("Created a new outbound connection [to={}]", msg.to());
                    }

                    NetworkClient<GossipProtocol> client = net.newClient();

                    client.setContext(msg.to());

                    holder = new EndpointHolder(client, true);

                    clients.put(nodeId, holder);

                    Connect connectMsg = new Connect(msg.from(), msg.to());

                    client.connect(msg.to().socket(), connectMsg, netClientCallback);
                }
            }

            if (TRACE) {
                log.trace("Sending message [outboundConnection={}, message={}]", holder.isOutbound(), msg);
            }

            holder.endpoint().send(msg, (sent, err, endpoint) -> {
                if (err.isPresent()) {
                    if (TRACE) {
                        log.trace("Failed to send a message [reason={}, message={}]", err.get(), sent);
                    }

                    callback.onSendFailure(sent, err.get());
                } else {
                    callback.onSendSuccess(sent);
                }

                if (onComplete != null) {
                    onComplete.run();
                }
            });
        }
    }

    public void sendAndDisconnect(GossipProtocol msg, Runnable onComplete) {
        Optional<Throwable> callbackErr = callback.onBeforeSend(msg);

        if (callbackErr.isPresent()) {
            callback.onSendFailure(msg, callbackErr.get());
        } else {
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
    }

    @Override
    public void onConnect(GossipProtocol msg, NetworkEndpoint<GossipProtocol> client) {
        if (msg.type() == GossipProtocol.Type.CONNECT) {
            if (DEBUG) {
                log.debug("Got a new inbound connection [from={}]", msg.from());
            }

            ClusterNodeId fromId = msg.from().id();

            client.setContext(msg.from());

            synchronized (mux) {
                if (clients.containsKey(fromId)) {
                    if (DEBUG) {
                        log.debug("Will not register inbound connection since another connection exists [from={}]", msg.from());
                    }
                } else {
                    if (DEBUG) {
                        log.debug("Registering inbound connection [from={}]", msg.from());
                    }

                    clients.put(fromId, new EndpointHolder(client, false));
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
        ClusterAddress address = client.getContext();

        if (address != null) {
            if (DEBUG) {
                log.debug("Closed inbound connection [from={}]", address);
            }

            synchronized (mux) {
                EndpointHolder holder = clients.get(address.id());

                // Remove from map only if it contains exactly the same client instance.
                if (holder != null && holder.endpoint() == client) {
                    if (DEBUG) {
                        log.debug("Removing inbound connection from registry [from={}]", address);
                    }

                    clients.remove(address.id());
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
