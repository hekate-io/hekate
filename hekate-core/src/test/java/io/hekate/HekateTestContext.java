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

package io.hekate;

import io.hekate.core.resource.ResourceLoadingException;
import io.hekate.core.resource.ResourceService;
import io.hekate.network.NetworkSslConfig;
import io.hekate.network.NetworkTransportType;
import io.netty.channel.epoll.Epoll;
import io.netty.handler.ssl.OpenSsl;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;

public class HekateTestContext {
    private static final String KEY_STORE_PASSWORD = "hekate-test1";

    private static final String KEY_STORE_PATH = "ssl/hekate-test1.jks";

    private final NetworkTransportType transport;

    private final Optional<NetworkSslConfig> ssl;

    private final ResourceService resources = path -> {
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(path);

        if (stream == null) {
            throw new ResourceLoadingException("Resource not found [path=" + path + ']');
        }

        return stream;
    };

    public HekateTestContext(NetworkTransportType transport, Optional<NetworkSslConfig> ssl) {
        this.transport = transport;
        this.ssl = ssl;
    }

    public HekateTestContext(HekateTestContext src) {
        this.transport = src.transport();
        this.ssl = src.ssl();
    }

    public static Stream<HekateTestContext> stream() {
        return get().stream();
    }

    public static Collection<HekateTestContext> get() {
        List<NetworkSslConfig> sslProvider = new ArrayList<>();

        // SSL disabled.
        if (HekateTestProps.is("NO_SSL_ENABLED")) {
            sslProvider.add(null);
        }

        // JDK SSL provider.
        if (HekateTestProps.is("SSL_JDK_ENABLED")) {
            sslProvider.add(new NetworkSslConfig()
                .withProvider(NetworkSslConfig.Provider.JDK)
                .withKeyStorePath(KEY_STORE_PATH)
                .withKeyStorePassword(KEY_STORE_PASSWORD)
                .withSslSessionCacheSize(1024)
                .withSslSessionCacheTimeout(10)
            );
        }

        // OpenSSL provider.
        if (HekateTestProps.is("SSL_OPENSSL_ENABLED") && OpenSsl.isAvailable()) {
            sslProvider.add(new NetworkSslConfig()
                .withProvider(NetworkSslConfig.Provider.OPEN_SSL)
                .withKeyStorePath(KEY_STORE_PATH)
                .withKeyStorePassword(KEY_STORE_PASSWORD)
                .withSslSessionCacheSize(1024)
                .withSslSessionCacheTimeout(10)
            );
        }

        List<NetworkTransportType> transports = new ArrayList<>();

        if (HekateTestProps.is("NIO_ENABLED")) {
            transports.add(NetworkTransportType.NIO);
        }

        if (HekateTestProps.is("EPOLL_ENABLED") && Epoll.isAvailable()) {
            transports.add(NetworkTransportType.EPOLL);
        }

        assertFalse("Network transports are not configured", transports.isEmpty());
        assertFalse("SSL transports are not configured", sslProvider.isEmpty());

        return transports.stream()
            .flatMap(transport ->
                sslProvider.stream().flatMap(ssl ->
                    Stream.of(
                        new HekateTestContext(transport, Optional.ofNullable(ssl))
                    )
                )
            ).collect(toList());
    }

    public static <T> List<T> map(Function<? super HekateTestContext, ? extends Stream<? extends T>> mapper) {
        return stream().flatMap(mapper).collect(toList());
    }

    public NetworkTransportType transport() {
        return transport;
    }

    public Optional<NetworkSslConfig> ssl() {
        return ssl;
    }

    public ResourceService resources() {
        return resources;
    }

    @Override
    public String toString() {
        return "transport=" + transport + ", ssl=" + ssl.map(NetworkSslConfig::getProvider).orElse(null);
    }
}
