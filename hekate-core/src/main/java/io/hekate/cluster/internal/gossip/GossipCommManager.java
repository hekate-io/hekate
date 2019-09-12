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

package io.hekate.cluster.internal.gossip;

import io.hekate.cluster.ClusterAddress;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.cluster.internal.gossip.GossipProtocol.GossipMessage;
import io.hekate.cluster.internal.gossip.GossipProtocol.LongTermConnect;
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
    private static class EndpointHolder {
        private final NetworkEndpoint<GossipProtocol> endpoint;

        private final boolean outbound;

        public EndpointHolder(NetworkEndpoint<GossipProtocol> endpoint, boolean outbound) {
            this.endpoint = endpoint;
            this.outbound = outbound;
        }

        public NetworkEndpoint<GossipProtocol> get() {
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

    private final ClusterAddress localAddress;

    private final NetworkConnector<GossipProtocol> connector;

    private final GossipCommListener listener;

    private final Object mux = new Object();

    private final Map<ClusterNodeId, EndpointHolder> cache = new HashMap<>();

    private final NetworkClientCallback<GossipProtocol> outboundCallback;

    public GossipCommManager(NetworkConnector<GossipProtocol> connector, ClusterAddress localAddress, GossipCommListener listener) {
        assert connector != null : "Network connector is null.";
        assert localAddress != null : "Local node address is null.";
        assert listener != null : "Gossip communication listener is null.";

        this.localAddress = localAddress;
        this.connector = connector;
        this.listener = listener;

        this.outboundCallback = new NetworkClientCallback<GossipProtocol>() {
            @Override
            public void onMessage(NetworkMessage<GossipProtocol> msg, NetworkClient<GossipProtocol> from) throws IOException {
                GossipProtocol gossip = msg.decode();

                if (TRACE) {
                    log.trace("Received message via outbound connection [message={}]", gossip);
                }

                listener.onReceive(gossip);
            }

            @Override
            public void onDisconnect(NetworkClient<GossipProtocol> from, Optional<Throwable> cause) {
                ClusterAddress addr = (ClusterAddress)from.getContext();

                if (addr != null) {
                    if (DEBUG) {
                        log.debug("Closed outbound connection [to={}]", addr);
                    }

                    synchronized (mux) {
                        EndpointHolder endpoint = cache.get(addr.id());

                        // Remove from map only if it contains exactly the same client instance.
                        if (endpoint != null && endpoint.get() == from) {
                            if (DEBUG) {
                                log.debug("Removing outbound connection from registry [to={}]", addr);
                            }

                            cache.remove(addr.id());
                        }
                    }

                    if (cause.isPresent()) {
                        // Notify on connect error only if we were not able to connect to the remote node's process.
                        // In case of timeout we can't decide on whether the remote process is alive or not (could be paused by GC, etc).
                        ConnectException connErr = ErrorUtils.findCause(ConnectException.class, cause.get());

                        if (connErr != null && !(connErr instanceof ConnectTimeoutException)) {
                            listener.onConnectFailure(addr);
                        }
                    }
                }
            }
        };
    }

    public void send(GossipMessage msg, Runnable onComplete) {
        Optional<Throwable> callbackErr = listener.onBeforeSend(msg);

        if (callbackErr.isPresent()) {
            Throwable err = callbackErr.get();

            if (TRACE) {
                log.trace("Failed to send a message [reason={}, message={}]", err, msg);
            }

            listener.onSendFailure(msg, err);
        } else {
            EndpointHolder endpoint = ensureConnected(msg.to());

            if (TRACE) {
                log.trace("Sending message [message={}]", msg);
            }

            endpoint.get().send(msg, (sent, err) -> {
                if (err == null) {
                    listener.onSendSuccess(sent);
                } else {
                    if (TRACE) {
                        log.trace("Failed to send a message [reason={}, message={}]", err, sent);
                    }

                    listener.onSendFailure(sent, err);
                }

                if (onComplete != null) {
                    onComplete.run();
                }
            });
        }
    }

    public void sendAndDisconnect(GossipProtocol msg) {
        Optional<Throwable> callbackErr = listener.onBeforeSend(msg);

        if (callbackErr.isPresent()) {
            listener.onSendFailure(msg, callbackErr.get());
        } else {
            NetworkClient<GossipProtocol> oneTimeClient = connector.newClient();

            oneTimeClient.connect(msg.toAddress(), msg, new NetworkClientCallback<GossipProtocol>() {
                @Override
                public void onMessage(NetworkMessage<GossipProtocol> message, NetworkClient<GossipProtocol> from) {
                    // No-op.
                }

                @Override
                public void onConnect(NetworkClient<GossipProtocol> client) {
                    client.disconnect();

                    listener.onSendSuccess(msg);
                }

                @Override
                public void onDisconnect(NetworkClient<GossipProtocol> client, Optional<Throwable> cause) {
                    cause.ifPresent(err ->
                        listener.onSendFailure(msg, err)
                    );
                }
            });
        }
    }

    @Override
    public void onConnect(GossipProtocol msg, NetworkEndpoint<GossipProtocol> client) {
        if (msg instanceof GossipMessage) {
            ClusterAddress from = msg.from();

            client.setContext(from);

            if (DEBUG) {
                log.debug("Got a new inbound connection [from={}]", from);
            }

            synchronized (mux) {
                if (cache.containsKey(from.id())) {
                    if (DEBUG) {
                        log.debug("Will not register inbound connection since another connection exists [from={}]", from);
                    }
                } else {
                    if (DEBUG) {
                        log.debug("Registering inbound connection [from={}]", from);
                    }

                    cache.put(from.id(), new EndpointHolder(client, false));
                }
            }

            if (msg.type() != GossipProtocol.Type.LONG_TERM_CONNECT) {
                listener.onReceive(msg);
            }
        } else {
            client.disconnect();

            listener.onReceive(msg);
        }
    }

    @Override
    public void onMessage(NetworkMessage<GossipProtocol> packet, NetworkEndpoint<GossipProtocol> from) throws IOException {
        GossipProtocol msg = packet.decode();

        if (TRACE) {
            log.trace("Received message via inbound connection [message={}]", msg);
        }

        listener.onReceive(msg);
    }

    @Override
    public void onDisconnect(NetworkEndpoint<GossipProtocol> from) {
        ClusterAddress addr = (ClusterAddress)from.getContext();

        if (addr != null) {
            if (DEBUG) {
                log.debug("Closed inbound connection [from={}]", addr);
            }

            synchronized (mux) {
                EndpointHolder endpoint = cache.get(addr.id());

                // Remove from map only if it contains exactly the same client instance.
                if (endpoint != null && endpoint.get() == from) {
                    if (DEBUG) {
                        log.debug("Removing inbound connection from registry [from={}]", addr);
                    }

                    cache.remove(addr.id());
                }
            }
        }
    }

    public void stop() {
        List<NetworkFuture<GossipProtocol>> discFutures;

        synchronized (mux) {
            discFutures = new ArrayList<>();

            // Make a copy in order to prevent concurrent modification errors.
            List<EndpointHolder> localCache = new ArrayList<>(cache.values());

            cache.clear();

            if (!localCache.isEmpty()) {
                if (DEBUG) {
                    log.debug("Closing connections  [size={}]", localCache.size());
                }

                // Disconnect endpoints.
                localCache.forEach(endpoint ->
                    discFutures.add(endpoint.get().disconnect())
                );
            }
        }

        // Await for clients termination.
        for (NetworkFuture<GossipProtocol> future : discFutures) {
            try {
                AsyncUtils.getUninterruptedly(future);
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause instanceof IOException) {
                    if (DEBUG) {
                        log.debug("Failed to close network connection due to an I/O error [cause={}]", cause.toString());
                    }
                } else {
                    log.warn("Failed to close network connection.", cause);
                }
            }
        }
    }

    private EndpointHolder ensureConnected(ClusterAddress to) {
        EndpointHolder endpoint;

        synchronized (mux) {
            endpoint = cache.get(to.id());

            if (endpoint == null) {
                if (DEBUG) {
                    log.debug("Created a new outbound connection [to={}]", to);
                }

                NetworkClient<GossipProtocol> client = connector.newClient();

                client.setContext(to);

                endpoint = new EndpointHolder(client, true);

                cache.put(to.id(), endpoint);

                client.connect(to.socket(), new LongTermConnect(localAddress, to), outboundCallback);
            }
        }

        return endpoint;
    }
}
