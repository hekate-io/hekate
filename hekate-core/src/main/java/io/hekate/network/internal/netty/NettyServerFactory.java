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

package io.hekate.network.internal.netty;

import io.hekate.network.internal.NetworkServerConfig;
import io.netty.channel.EventLoopGroup;
import java.util.LinkedList;
import java.util.List;

public class NettyServerFactory extends NetworkServerConfig {
    private boolean disableHeartbeats;

    private EventLoopGroup acceptorEventLoopGroup;

    private EventLoopGroup workerEventLoopGroup;

    private List<NettyServerHandlerConfig<?>> handlers;

    public boolean isDisableHeartbeats() {
        return disableHeartbeats;
    }

    public void setDisableHeartbeats(boolean disableHeartbeats) {
        this.disableHeartbeats = disableHeartbeats;
    }

    public List<NettyServerHandlerConfig<?>> getHandlers() {
        return handlers;
    }

    public void setHandlers(List<NettyServerHandlerConfig<?>> handlers) {
        this.handlers = handlers;
    }

    public void addHandler(NettyServerHandlerConfig<?> handler) {
        if (handlers == null) {
            handlers = new LinkedList<>();
        }

        handlers.add(handler);
    }

    public boolean removeHandler(NettyServerHandlerConfig<?> handler) {
        return handlers != null && handlers.remove(handler);
    }

    public EventLoopGroup getAcceptorEventLoopGroup() {
        return acceptorEventLoopGroup;
    }

    public void setAcceptorEventLoopGroup(EventLoopGroup acceptorEventLoopGroup) {
        this.acceptorEventLoopGroup = acceptorEventLoopGroup;
    }

    public EventLoopGroup getWorkerEventLoopGroup() {
        return workerEventLoopGroup;
    }

    public void setWorkerEventLoopGroup(EventLoopGroup workerEventLoopGroup) {
        this.workerEventLoopGroup = workerEventLoopGroup;
    }

    public NettyServer createServer() {
        return new NettyServer(this);
    }
}
