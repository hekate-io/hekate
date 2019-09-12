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

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerFactoryBase;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.util.ArrayList;
import java.util.List;

/**
 * Configurable factory for Netty-based {@link NetworkServer}s.
 */
public class NettyServerFactory extends NetworkServerFactoryBase {
    private boolean disableHeartbeats;

    private EventLoopGroup acceptorEventLoop;

    private EventLoopGroup workerEventLoop;

    private List<NettyServerHandlerConfig<?>> handlers;

    private SslContext ssl;

    private NettyMetricsFactory metrics;

    /**
     * Returns {@code true} if heartbeats are disabled (see {@link #setDisableHeartbeats(boolean)}).
     *
     * @return {@code true} if heartbeats are disabled.
     */
    public boolean isDisableHeartbeats() {
        return disableHeartbeats;
    }

    /**
     * Enabled/disabled heartbeats support.
     *
     * <p>
     * If heartbeats are disabled then {@link NetworkServer} will not use heartbeats to keep connections alive. In such case it is up to the
     * application to manage heartbeats and perform detection of broken connections.
     * </p>
     *
     * <p>
     * Default value of this parameter is {@code false} (i.e. heartbeats are enabled).
     * </p>
     *
     * @param disableHeartbeats {@code true} if heartbeats must be disabled.
     */
    public void setDisableHeartbeats(boolean disableHeartbeats) {
        this.disableHeartbeats = disableHeartbeats;
    }

    /**
     * Fluent-style version of {@link #setDisableHeartbeats(boolean)}.
     *
     * @param disableHeartbeats {@code true} if heartbeats must be disabled.
     *
     * @return This instance.
     */
    public NettyServerFactory withDisableHeartbeats(boolean disableHeartbeats) {
        setDisableHeartbeats(disableHeartbeats);

        return this;
    }

    /**
     * Returns the list of server connection handlers (see {@link #setHandlers(List)}).
     *
     * @return Server handlers.
     */
    public List<NettyServerHandlerConfig<?>> getHandlers() {
        return handlers;
    }

    /**
     * Sets the list of server connection handlers.
     *
     * @param handlers Server handlers.
     */
    public void setHandlers(List<NettyServerHandlerConfig<?>> handlers) {
        this.handlers = handlers;
    }

    /**
     * Fluent-style version of {@link #setHandlers(List)}.
     *
     * @param handler Server handler.
     *
     * @return This instance.
     */
    public NettyServerFactory withHandler(NettyServerHandlerConfig<?> handler) {
        if (handlers == null) {
            handlers = new ArrayList<>();
        }

        handlers.add(handler);

        return this;
    }

    /**
     * Returns the event loop group that is responsible for accepting new connections (see {@link #setAcceptorEventLoop(EventLoopGroup)}).
     *
     * @return Event loop group for accepting new connections.
     */
    public EventLoopGroup getAcceptorEventLoop() {
        return acceptorEventLoop;
    }

    /**
     * Sets the event loop group that will be responsible for accepting new connections.
     *
     * @param acceptorEventLoop Event loop group for accepting new connections.
     */
    public void setAcceptorEventLoop(EventLoopGroup acceptorEventLoop) {
        this.acceptorEventLoop = acceptorEventLoop;
    }

    /**
     * Fluent-style version of {@link #setAcceptorEventLoop(EventLoopGroup)}.
     *
     * @param acceptorEventLoop Event loop group for accepting new connections.
     *
     * @return This instance.
     */
    public NettyServerFactory withAcceptorEventLoop(EventLoopGroup acceptorEventLoop) {
        setAcceptorEventLoop(acceptorEventLoop);

        return this;
    }

    /**
     * Returns the event loop group that is responsible for handling messaging operations (see {@link #setWorkerEventLoop(EventLoopGroup)}).
     *
     * @return Event loop group for messaging operations handling.
     */
    public EventLoopGroup getWorkerEventLoop() {
        return workerEventLoop;
    }

    /**
     * Sets the event loop group that will be responsible for handling messaging operations.
     *
     * @param workerEventLoop Event loop group for messaging operations handling.
     */
    public void setWorkerEventLoop(EventLoopGroup workerEventLoop) {
        this.workerEventLoop = workerEventLoop;
    }

    /**
     * Fluent-style version of {@link #setWorkerEventLoop(EventLoopGroup)}.
     *
     * @param workerEventLoop Event loop group for messaging operations handling.
     *
     * @return This instance.
     */
    public NettyServerFactory withWorkerEventLoop(EventLoopGroup workerEventLoop) {
        setWorkerEventLoop(workerEventLoop);

        return this;
    }

    /**
     * Returns the SSL context (see {@link #setSsl(SslContext)}).
     *
     * @return SSL context.
     */
    public SslContext getSsl() {
        return ssl;
    }

    /**
     * Sets the SSL context that should be used to secure all network communications.
     *
     * <p>
     * This parameter is optional and if not specified then no encryption will be applied.
     * </p>
     *
     * <p>
     * <b>Note:</b> If SSL is configured for the server then it should be configured on the client side too (see {@link
     * NettyClientFactory#setSsl(SslContext)}).
     * </p>
     *
     * @param ssl SSL context.
     */
    public void setSsl(SslContext ssl) {
        ArgAssert.check(ssl == null || !ssl.isClient(), "SSL context must be configured in server mode.");

        this.ssl = ssl;
    }

    /**
     * Fluent-style version of {@link #setSsl(SslContext)}.
     *
     * @param ssl SSL context.
     *
     * @return SSL context.
     */
    public NettyServerFactory withSsl(SslContext ssl) {
        setSsl(ssl);

        return this;
    }

    /**
     * Returns the metrics factory (see {@link #setMetrics(NettyMetricsFactory)}).
     *
     * @return Metrics factory.
     */
    public NettyMetricsFactory getMetrics() {
        return metrics;
    }

    /**
     * Sets the metrics factory.
     *
     * <p>
     * This parameter is optional and if not specified then no metrics will be collected by the {@link NetworkServer}.
     * </p>
     *
     * @param metrics Metrics factory.
     */
    public void setMetrics(NettyMetricsFactory metrics) {
        this.metrics = metrics;
    }

    /**
     * Fluent-style version of {@link #setMetrics(NettyMetricsFactory)}.
     *
     * @param metrics Metrics factory.
     *
     * @return This instance.
     */
    public NettyServerFactory withMetrics(NettyMetricsFactory metrics) {
        setMetrics(metrics);

        return this;
    }

    @Override
    public NetworkServer createServer() {
        return new NettyServer(this);
    }
}
