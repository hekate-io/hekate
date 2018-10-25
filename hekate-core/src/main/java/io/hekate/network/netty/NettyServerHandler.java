package io.hekate.network.netty;

import io.hekate.network.NetworkEndpoint;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

class NettyServerHandler {
    private final NettyServerHandlerConfig<Object> config;

    private final NettyMetricsSink metrics;

    private final Map<NettyServerClient, Void> clients = new IdentityHashMap<>();

    public NettyServerHandler(NettyServerHandlerConfig<Object> config, NettyMetricsSink metrics) {
        this.config = config;
        this.metrics = metrics;
    }

    public NettyServerHandlerConfig<Object> config() {
        return config;
    }

    public NettyMetricsSink metrics() {
        return metrics;
    }

    public List<NetworkEndpoint<?>> clients() {
        synchronized (clients) {
            if (clients.isEmpty()) {
                return Collections.emptyList();
            } else {
                return new ArrayList<>(clients.keySet());
            }
        }
    }

    public void add(NettyServerClient client) {
        synchronized (clients) {
            clients.put(client, null);
        }
    }

    public void remove(NettyServerClient client) {
        synchronized (clients) {
            clients.remove(client, null);
        }
    }
}
