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

package io.hekate.network.internal;

import io.hekate.HekateTestBase;
import io.hekate.HekateTestContext;
import io.hekate.network.NetworkClient;
import io.hekate.network.NetworkServer;
import io.hekate.network.NetworkServerHandler;
import io.hekate.network.NetworkTransportType;
import io.hekate.network.netty.NettyClientFactory;
import io.hekate.network.netty.NettyServerFactory;
import io.hekate.network.netty.NettyServerHandlerConfig;
import io.hekate.network.netty.NettyUtils;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.SslContext;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public abstract class NetworkTestBase extends HekateTestBase {
    public interface ServerConfigurer {
        void configure(NettyServerFactory factory);
    }

    public interface ClientConfigurer<T> {
        void configure(NettyClientFactory<T> factory);
    }

    public interface HandlerConfigurer<T> {
        void configure(NettyServerHandlerConfig<T> handler);
    }

    public static final String TEST_PROTOCOL = "test";

    public static final int NIO_THREADS = 8;

    private final AtomicInteger portsSeq = new AtomicInteger(10000);

    private final List<NetworkServer> servers = new CopyOnWriteArrayList<>();

    private final List<NetworkClient<?>> clients = new CopyOnWriteArrayList<>();

    private final HekateTestContext ctx;

    private EventLoopGroup acceptorThreads;

    private EventLoopGroup nioServerThreads;

    private EventLoopGroup nioClientThreads;

    private SslContext clientSsl;

    private SslContext serverSsl;

    public NetworkTestBase(HekateTestContext ctx) {
        this.ctx = ctx;
    }

    @Parameters(name = "{index}: {0}")
    public static Collection<HekateTestContext> getNetworkTestContexts() {
        return HekateTestContext.all();
    }

    @Before
    public void setUp() throws Exception {
        acceptorThreads = newEventLoop(1);
        nioServerThreads = newEventLoop(NIO_THREADS);
        nioClientThreads = newEventLoop(NIO_THREADS);

        clientSsl = ctx.ssl().map(cfg -> NettySslUtils.clientContext(cfg, ctx.resources())).orElse(null);
        serverSsl = ctx.ssl().map(cfg -> NettySslUtils.serverContext(cfg, ctx.resources())).orElse(null);
    }

    @After
    public void tearDown() throws Exception {
        portsSeq.set(0);

        try {
            for (NetworkClient<?> client : clients) {
                client.disconnect().get();
            }

            for (NetworkServer server : servers) {
                server.stop().get();
            }

            servers.clear();
        } finally {
            NettyUtils.shutdown(acceptorThreads).awaitUninterruptedly();
            NettyUtils.shutdown(nioServerThreads).awaitUninterruptedly();
            NettyUtils.shutdown(nioClientThreads).awaitUninterruptedly();
        }
    }

    protected HekateTestContext context() {
        return ctx;
    }

    protected EventLoopGroup newEventLoop(int thread) {
        if (context().transport() == NetworkTransportType.EPOLL) {
            return new EpollEventLoopGroup(thread);
        } else {
            return new NioEventLoopGroup(thread);
        }
    }

    protected InetSocketAddress newServerAddress() throws UnknownHostException {
        return new InetSocketAddress(InetAddress.getLocalHost(), 10001 + portsSeq.incrementAndGet());
    }

    protected NetworkServer createAndConfigureServerHandler(HandlerConfigurer<String> handler) {
        return createAndConfigureServer(createHandler((msg, from) -> { /* No-op. */ }), handler, null);
    }

    protected NetworkServer createAndConfigureServerHandler(HandlerConfigurer<String> handler, ServerConfigurer server) {
        return createAndConfigureServer(createHandler((msg, from) -> { /* No-op. */ }), handler, server);
    }

    protected NetworkServer createServer() {
        return createServer(createHandler((msg, from) -> { /* No-op. */ }));
    }

    protected NetworkServer createServer(NettyServerHandlerConfig<String> handlerCfg) {
        return createAndConfigureServer(handlerCfg, null, null);
    }

    protected NetworkServer createServer(NetworkServerHandler<String> handler) {
        return createAndConfigureServer(createHandler(handler), null, null);
    }

    protected NetworkServer createAndConfigureServer(ServerConfigurer configurer) {
        return createAndConfigureServer(createHandler((msg, from) -> { /* No-op. */ }), null, configurer);
    }

    protected NetworkServer createAndConfigureServer(NettyServerHandlerConfig<String> handler, HandlerConfigurer<String> handlerConfigurer,
        ServerConfigurer serverConfigurer) {
        if (handlerConfigurer != null) {
            handlerConfigurer.configure(handler);
        }

        NettyServerFactory factory = new NettyServerFactory();

        factory.setHeartbeatInterval(context().hbInterval());
        factory.setHeartbeatLossThreshold(context().hbLossThreshold());

        factory.setAcceptorEventLoop(acceptorThreads);
        factory.setWorkerEventLoop(nioServerThreads);
        factory.setSsl(serverSsl);

        factory.withHandler(handler);

        if (serverConfigurer != null) {
            serverConfigurer.configure(factory);
        }

        NetworkServer server = factory.createServer();

        servers.add(server);

        return server;
    }

    protected NettyServerHandlerConfig<String> createHandler(NetworkServerHandler<String> handler) {
        return createHandler(TEST_PROTOCOL, handler);
    }

    protected NettyServerHandlerConfig<String> createHandler(String protocol, NetworkServerHandler<String> handler) {
        NettyServerHandlerConfig<String> handlerCfg = new NettyServerHandlerConfig<>();

        handlerCfg.setProtocol(protocol);
        handlerCfg.setCodecFactory(createStringCodecFactory());
        handlerCfg.setHandler(handler);

        return handlerCfg;
    }

    protected NetworkClient<String> createClient() {
        return createClient(null);
    }

    protected NetworkClient<String> createClient(ClientConfigurer<String> configurer) {
        NettyClientFactory<String> factory = new NettyClientFactory<>();

        factory.setProtocol(TEST_PROTOCOL);
        factory.setCodecFactory(createStringCodecFactory());
        factory.setConnectTimeout(context().connectTimeout());
        factory.setEventLoop(nioClientThreads);
        factory.setSsl(clientSsl);

        if (configurer != null) {
            configurer.configure(factory);
        }

        NetworkClient<String> client = factory.newClient();

        clients.add(client);

        return client;
    }
}
