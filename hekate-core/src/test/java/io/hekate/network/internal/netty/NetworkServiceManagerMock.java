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

import io.hekate.core.HekateException;
import io.hekate.core.service.ConfigurableService;
import io.hekate.core.service.ConfigurationContext;
import io.hekate.core.service.DependencyContext;
import io.hekate.core.service.DependentService;
import io.hekate.core.service.InitializationContext;
import io.hekate.core.service.InitializingService;
import io.hekate.core.service.TerminatingService;
import io.hekate.network.NetworkConnector;
import io.hekate.network.PingCallback;
import io.hekate.network.internal.NetworkBindCallback;
import io.hekate.network.internal.NetworkServerFuture;
import io.hekate.network.internal.NetworkServiceManager;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertNotNull;

public class NetworkServiceManagerMock implements NetworkServiceManager, DependentService, ConfigurableService, InitializingService,
    TerminatingService {
    private final NettyNetworkService delegate;

    private volatile NettyServer server;

    public NetworkServiceManagerMock(NettyNetworkService delegate) {
        this.delegate = delegate;
    }

    @Override
    public NetworkServerFuture bind(NetworkBindCallback callback) throws HekateException {
        NetworkServerFuture future = delegate.bind(callback);

        future.whenComplete((netServer, throwable) -> server = (NettyServer)netServer);

        return future;
    }

    public void start() {
        delegate.start();
    }

    public void stop() {
        delegate.stop();
    }

    public void fireServerFailure(Throwable error) {
        NettyServer server = this.server;

        assertNotNull(server);

        server.getServerChannel().pipeline().fireExceptionCaught(error);
    }

    @Override
    public <T> NetworkConnector<T> get(String protocol) throws IllegalArgumentException {
        return delegate.get(protocol);
    }

    @Override
    public boolean has(String protocol) {
        return delegate.has(protocol);
    }

    @Override
    public void ping(InetSocketAddress address, PingCallback callback) {
        delegate.ping(address, callback);
    }

    @Override
    public void resolve(DependencyContext ctx) {
        delegate.resolve(ctx);
    }

    @Override
    public void configure(ConfigurationContext ctx) {
        delegate.configure(ctx);
    }

    @Override
    public void terminate() throws HekateException {
        delegate.terminate();
    }

    @Override
    public void preTerminate() throws HekateException {
        delegate.preTerminate();
    }

    @Override
    public void postTerminate() throws HekateException {
        delegate.postTerminate();
    }

    @Override
    public void initialize(InitializationContext ctx) throws HekateException {
        delegate.initialize(ctx);
    }

    @Override
    public void preInitialize(InitializationContext ctx) throws HekateException {
        delegate.preInitialize(ctx);
    }

    @Override
    public void postInitialize(InitializationContext ctx) throws HekateException {
        delegate.postInitialize(ctx);
    }
}
