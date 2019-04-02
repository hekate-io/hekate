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

package io.hekate.network.netty;

import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkServerHandlerConfig;
import io.netty.channel.EventLoopGroup;
import java.util.List;

/**
 * Netty-specific extension of {@link NetworkServerHandlerConfig}.
 *
 * @param <T> Base type of messages that can be processed by {@link NetworkServerHandler}.
 *
 * @see NettyServerFactory#setHandlers(List)
 */
public class NettyServerHandlerConfig<T> extends NetworkServerHandlerConfig<T> {
    private EventLoopGroup eventLoop;

    /**
     * Returns the event loop of this handler (see {@link #setEventLoop(EventLoopGroup)}).
     *
     * @return Event loop.
     */
    public EventLoopGroup getEventLoop() {
        return eventLoop;
    }

    /**
     * Sets the event loop to be used by this handler.
     *
     * <p>
     * If not specified then all network operations will be handled by the the {@link NetworkServer}'s
     * {@link NettyServerFactory#setWorkerEventLoop(EventLoopGroup) core event loop}.
     * </p>
     *
     * @param eventLoop Event loop.
     */
    public void setEventLoop(EventLoopGroup eventLoop) {
        this.eventLoop = eventLoop;
    }

    /**
     * Fluent-style version of {@link #setEventLoop(EventLoopGroup)}.
     *
     * @param eventLoop Event loop.
     *
     * @return This instance.
     */
    public NettyServerHandlerConfig<T> withEventLoop(EventLoopGroup eventLoop) {
        setEventLoop(eventLoop);

        return this;
    }
}
