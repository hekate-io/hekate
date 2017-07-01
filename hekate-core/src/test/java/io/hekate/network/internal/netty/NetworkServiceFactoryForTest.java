package io.hekate.network.internal.netty;

import io.hekate.network.NetworkService;
import io.hekate.network.NetworkServiceFactory;

public class NetworkServiceFactoryForTest extends NetworkServiceFactory {
    private NettySpyForTest spy;

    public NettySpyForTest getSpy() {
        return spy;
    }

    public void setSpy(NettySpyForTest spy) {
        this.spy = spy;
    }

    @Override
    public NetworkService createService() {
        return new NettyNetworkService(this) {
            @Override
            protected <T> NettyClientFactory<T> createClientFactory() {
                NettyClientFactory<T> factory = super.createClientFactory();

                factory.setSpy(spy);

                return factory;
            }
        };
    }
}
