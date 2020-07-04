/*
 * Copyright 2020 The Hekate Project
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

package io.hekate.network.netty;

import io.hekate.network.NetworkEndpoint;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

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
                return emptyList();
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
