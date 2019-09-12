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

import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.ConfigCheck;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkClientCallback;
import io.hekate.network.NetworkEndpoint;
import io.hekate.network.NetworkFuture;
import io.hekate.network.NetworkSendCallback;
import io.hekate.network.internal.NettyChannelSupport;
import io.netty.channel.Channel;
import io.netty.channel.EventLoop;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import io.netty.util.internal.ThrowableUtil;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.hekate.network.NetworkClient.State.CONNECTED;
import static io.hekate.network.NetworkClient.State.CONNECTING;
import static io.hekate.network.NetworkClient.State.DISCONNECTED;

class NettyClient<T> implements NetworkClient<T>, NettyChannelSupport {
    private static final ClosedChannelException CLOSED_CHANNEL_EXCEPTION = ThrowableUtil.unknownStackTrace(
        new ClosedChannelException(), NettyClient.class, "doSend()"
    );

    private final AtomicReference<NettyClientContext<T>> ctx = new AtomicReference<>();

    private final Object mux = new Object();

    private final CodecFactory<Object> codecFactory;

    private final String protocol;

    private final Integer connectTimeout;

    private final long idleTimeout;

    private final boolean tcpNoDelay;

    private final Integer soReceiveBufSize;

    private final Integer soSendBufSize;

    private final Boolean soReuseAddress;

    private final NettyMetricsSink metrics;

    private final Logger log;

    private final boolean epoll;

    private final EventLoop eventLoop;

    private final int affinity;

    private final SslContext ssl;

    private final NettySpy spy;

    private volatile Object userCtx;

    @SuppressWarnings("unchecked")
    public NettyClient(NettyClientFactory<T> factory) {
        assert factory != null : "Configuration is null.";

        ConfigCheck check = ConfigCheck.get(NettyClientFactory.class);

        check.notEmpty(factory.getProtocol(), "protocol");
        check.validSysName(factory.getProtocol(), "protocol");
        check.notNull(factory.getCodecFactory(), "codec factory");
        check.notNull(factory.getEventLoop(), "event loops group");

        if (factory.getLoggerCategory() == null) {
            log = LoggerFactory.getLogger(NettyClient.class);
        } else {
            log = LoggerFactory.getLogger(factory.getLoggerCategory());
        }

        affinity = ThreadLocalRandom.current().nextInt();

        connectTimeout = factory.getConnectTimeout();
        idleTimeout = factory.getIdleTimeout();
        tcpNoDelay = factory.isTcpNoDelay();
        soReceiveBufSize = factory.getSoReceiveBufferSize();
        soSendBufSize = factory.getSoSendBufferSize();
        soReuseAddress = factory.getSoReuseAddress();
        codecFactory = (CodecFactory<Object>)factory.getCodecFactory();
        protocol = factory.getProtocol();
        epoll = factory.getEventLoop() instanceof EpollEventLoopGroup;
        eventLoop = factory.getEventLoop().next();
        metrics = factory.getMetrics();
        ssl = factory.getSsl();
        spy = factory.getSpy();
    }

    @Override
    public String protocol() {
        return protocol;
    }

    @Override
    public InetSocketAddress remoteAddress() {
        NettyClientContext localCtx = ctx.get();

        return localCtx != null ? localCtx.remoteAddress() : null;
    }

    @Override
    public InetSocketAddress localAddress() {
        NettyClientContext localCtx = ctx.get();

        return localCtx != null ? localCtx.localAddress() : null;
    }

    @Override
    public boolean isSecure() {
        return ssl != null;
    }

    @Override
    public Object getContext() {
        return userCtx;
    }

    @Override
    public void setContext(Object ctx) {
        this.userCtx = ctx;
    }

    @Override
    public NetworkFuture<T> connect(InetSocketAddress address, NetworkClientCallback<T> callback) {
        return connect(address, null, callback);
    }

    @Override
    public NetworkFuture<T> connect(InetSocketAddress address, T login, NetworkClientCallback<T> callback) {
        ArgAssert.notNull(address, "Address");
        ArgAssert.notNull(callback, "Callback");

        synchronized (mux) {
            NettyClientContext<T> oldCtx = ctx.get();

            if (oldCtx != null && (oldCtx.state() == CONNECTING || oldCtx.state() == CONNECTED)) {
                throw new IllegalStateException("Already connected [address=" + address + ']');
            }

            if (eventLoop.isTerminated()) {
                throw new IllegalStateException("I/O thread pool terminated.");
            }

            // Prepare codec.
            Codec<Object> codec = codecFactory.createCodec();

            // Initialize context.
            NettyClientContext<T> newCtx = new NettyClientContext<>(
                address,
                codec,
                metrics,
                this,
                eventLoop,
                protocol,
                affinity,
                login,
                connectTimeout,
                idleTimeout,
                ssl,
                epoll,
                tcpNoDelay,
                soReceiveBufSize,
                soSendBufSize,
                soReuseAddress,
                spy,
                log,
                callback
            );

            // Set the current active context.
            this.ctx.set(newCtx);

            // Cleanup on disconnect.
            newCtx.disconnectFuture().whenComplete((endpoint, err) ->
                // Nullify only if this is the same context object.
                ctx.compareAndSet(newCtx, null)
            );

            // Connect.
            return newCtx.connect();
        }
    }

    @Override
    public State state() {
        NettyClientContext localCtx = ctx.get();

        return localCtx == null ? DISCONNECTED : localCtx.state();
    }

    @Override
    public void close() {
        disconnect().join();
    }

    @Override
    public void send(T msg) {
        doSend(msg, null);
    }

    @Override
    public void send(T msg, NetworkSendCallback<T> callback) {
        doSend(msg, callback);
    }

    @Override
    public void pauseReceiving(Consumer<NetworkEndpoint<T>> callback) {
        NettyClientContext<T> localCtx = ctx.get();

        if (localCtx == null) {
            callback.accept(this);
        } else {
            localCtx.pauseReceiver(true, callback);
        }
    }

    @Override
    public void resumeReceiving(Consumer<NetworkEndpoint<T>> callback) {
        NettyClientContext<T> localCtx = ctx.get();

        if (localCtx == null) {
            callback.accept(this);
        } else {
            localCtx.pauseReceiver(false, callback);
        }
    }

    @Override
    public boolean isReceiving() {
        NettyClientContext localCtx = ctx.get();

        return localCtx != null && localCtx.channel().config().isAutoRead();
    }

    @Override
    public NetworkFuture<T> disconnect() {
        NettyClientContext<T> localCtx = ctx.get();

        if (localCtx == null) {
            return NetworkFuture.completed(this);
        } else {
            return localCtx.disconnect();
        }
    }

    @Override
    public Optional<Channel> nettyChannel() {
        NettyClientContext localCtx = ctx.get();

        return localCtx != null ? Optional.of(localCtx.channel()) : Optional.empty();
    }

    private void doSend(T msg, NetworkSendCallback<T> onSend) {
        NettyClientContext<T> localCtx = ctx.get();

        if (localCtx == null) {
            // Notify on channel close error.
            if (onSend != null) {
                NettyUtils.runAtAllCost(eventLoop, () ->
                    notifyOnError(msg, onSend, CLOSED_CHANNEL_EXCEPTION)
                );
            }
        } else {
            // Write message.
            localCtx.write(msg, onSend);
        }
    }

    private void notifyOnError(T msg, NetworkSendCallback<T> onSend, Throwable error) {
        try {
            onSend.onComplete(msg, error);
        } catch (RuntimeException | Error e) {
            log.error("Failed to notify callback on network operation failure.", e);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "["
            + "protocol=" + protocol
            + ", connection=" + ctx
            + ']';
    }
}
