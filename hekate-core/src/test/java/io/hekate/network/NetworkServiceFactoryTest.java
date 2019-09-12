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

package io.hekate.network;

import io.hekate.HekateTestBase;
import io.hekate.network.address.AddressPattern;
import io.hekate.network.address.AddressSelector;
import java.util.Collections;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class NetworkServiceFactoryTest extends HekateTestBase {
    private final NetworkServiceFactory cfg = new NetworkServiceFactory();

    @Test
    public void testPort() {
        assertEquals(NetworkServiceFactory.DEFAULT_PORT, cfg.getPort());

        cfg.setPort(10000);

        assertEquals(10000, cfg.getPort());

        assertEquals(10001, cfg.withPort(10001).getPort());
    }

    @Test
    public void testPortRange() {
        assertEquals(NetworkServiceFactory.DEFAULT_PORT_RANGE, cfg.getPortRange());

        cfg.setPortRange(10);

        assertEquals(10, cfg.getPortRange());

        assertEquals(11, cfg.withPortRange(11).getPortRange());
    }

    @Test
    public void testHost() {
        assertNotNull(cfg.getHostSelector());

        AddressSelector s1 = new AddressPattern();
        AddressSelector s2 = new AddressPattern();

        cfg.setHostSelector(s1);

        assertSame(s1, cfg.getHostSelector());

        assertSame(cfg, cfg.withHostSelector(s2));
        assertSame(s2, cfg.getHostSelector());
    }

    @Test
    public void testHostPattern() {
        AddressSelector old = cfg.getHostSelector();

        assertNotNull(old);
        assertTrue(old.getClass().getName(), old instanceof AddressPattern);
        assertEquals("any-ip4", cfg.getHost());

        cfg.setHost("127.0.0.1");

        assertEquals("127.0.0.1", cfg.getHost());
        assertNotSame(old, cfg.getHostSelector());

        old = cfg.getHostSelector();

        assertSame(cfg, cfg.withHost("127.0.0.2"));

        assertEquals("127.0.0.2", cfg.getHost());
        assertNotSame(old, cfg.getHostSelector());

        old = cfg.getHostSelector();

        cfg.setHost(null);

        assertEquals("any-ip4", cfg.getHost());
        assertNotSame(old, cfg.getHostSelector());
    }

    @Test
    public void testConnectTimeout() {
        assertEquals(NetworkServiceFactory.DEFAULT_CONNECT_TIMEOUT, cfg.getConnectTimeout());

        cfg.setConnectTimeout(100);

        assertEquals(100, cfg.getConnectTimeout());

        assertSame(cfg, cfg.withConnectTimeout(1000));
        assertEquals(1000, cfg.getConnectTimeout());
    }

    @Test
    public void testAcceptRetryInterval() {
        assertEquals(NetworkServiceFactory.DEFAULT_ACCEPT_RETRY_INTERVAL, cfg.getAcceptRetryInterval());

        cfg.setAcceptRetryInterval(NetworkServiceFactory.DEFAULT_ACCEPT_RETRY_INTERVAL + 1000);

        assertEquals(NetworkServiceFactory.DEFAULT_ACCEPT_RETRY_INTERVAL + 1000, cfg.getAcceptRetryInterval());

        assertSame(cfg, cfg.withAcceptRetryInterval(1000));
        assertEquals(1000, cfg.getAcceptRetryInterval());
    }

    @Test
    public void testHeartbeatInterval() {
        assertEquals(NetworkServiceFactory.DEFAULT_HB_INTERVAL, cfg.getHeartbeatInterval());

        cfg.setHeartbeatInterval(NetworkServiceFactory.DEFAULT_HB_INTERVAL + 1000);

        assertEquals(NetworkServiceFactory.DEFAULT_HB_INTERVAL + 1000, cfg.getHeartbeatInterval());

        assertSame(cfg, cfg.withHeartbeatInterval(10001));
        assertEquals(10001, cfg.getHeartbeatInterval());
    }

    @Test
    public void testHeartbeatLossThreshold() {
        assertEquals(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD, cfg.getHeartbeatLossThreshold());

        cfg.setHeartbeatLossThreshold(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD + 1000);

        assertEquals(NetworkServiceFactory.DEFAULT_HB_LOSS_THRESHOLD + 1000, cfg.getHeartbeatLossThreshold());

        assertSame(cfg, cfg.withHeartbeatLossThreshold(10002));
        assertEquals(10002, cfg.getHeartbeatLossThreshold());
    }

    @Test
    public void testNioThreads() {
        assertEquals(Runtime.getRuntime().availableProcessors(), cfg.getNioThreads());

        cfg.setNioThreads(Runtime.getRuntime().availableProcessors() + 1000);

        assertEquals(Runtime.getRuntime().availableProcessors() + 1000, cfg.getNioThreads());

        assertSame(cfg, cfg.withNioThreads(1));
        assertEquals(1, cfg.getNioThreads());
    }

    @Test
    public void testTransport() {
        assertSame(NetworkTransportType.AUTO, cfg.getTransport());

        cfg.setTransport(NetworkTransportType.EPOLL);

        assertSame(NetworkTransportType.EPOLL, cfg.getTransport());

        assertSame(cfg, cfg.withTransport(NetworkTransportType.NIO));
        assertSame(NetworkTransportType.NIO, cfg.getTransport());
    }

    @Test
    public void testTcpNoDelay() {
        assertTrue(cfg.isTcpNoDelay());

        cfg.setTcpNoDelay(true);

        assertTrue(cfg.isTcpNoDelay());

        cfg.setTcpNoDelay(false);

        assertFalse(cfg.isTcpNoDelay());

        assertSame(cfg, cfg.withTcpNoDelay(true));
        assertTrue(cfg.isTcpNoDelay());
    }

    @Test
    public void testTcpReceiveBufferSize() {
        assertNull(cfg.getTcpReceiveBufferSize());

        cfg.setTcpReceiveBufferSize(1000);

        assertEquals(1000, cfg.getTcpReceiveBufferSize().intValue());

        cfg.setTcpReceiveBufferSize(null);

        assertNull(cfg.getTcpReceiveBufferSize());

        assertSame(cfg, cfg.withTcpReceiveBufferSize(100));
        assertEquals(100, cfg.getTcpReceiveBufferSize().intValue());
    }

    @Test
    public void testTcpSendBufferSize() {
        assertNull(cfg.getTcpSendBufferSize());

        cfg.setTcpSendBufferSize(1000);

        assertEquals(1000, cfg.getTcpSendBufferSize().intValue());

        cfg.setTcpSendBufferSize(null);

        assertNull(cfg.getTcpSendBufferSize());

        assertSame(cfg, cfg.withTcpSendBufferSize(1001));
        assertEquals(1001, cfg.getTcpSendBufferSize().intValue());
    }

    @Test
    public void testTcpReuseAddress() {
        assertNull(cfg.getTcpReuseAddress());

        cfg.setTcpReuseAddress(true);

        assertTrue(cfg.getTcpReuseAddress());

        cfg.setTcpReuseAddress(false);

        assertFalse(cfg.getTcpReuseAddress());

        cfg.setTcpReuseAddress(null);

        assertNull(cfg.getTcpReuseAddress());

        assertSame(cfg, cfg.withTcpReuseAddress(true));
        assertTrue(cfg.getTcpReuseAddress());
    }

    @Test
    public void testTcpBacklog() {
        assertNull(cfg.getTcpBacklog());

        cfg.setTcpBacklog(1000);

        assertEquals(1000, cfg.getTcpBacklog().intValue());

        cfg.setTcpBacklog(null);

        assertNull(cfg.getTcpBacklog());

        assertSame(cfg, cfg.withTcpBacklog(101));
        assertEquals(101, cfg.getTcpBacklog().intValue());
    }

    @Test
    public void testConnectors() {
        assertNull(cfg.getConnectors());

        NetworkConnectorConfig<Object> m1 = new NetworkConnectorConfig<>();
        NetworkConnectorConfig<Object> m2 = new NetworkConnectorConfig<>();

        cfg.setConnectors(Collections.singletonList(m1));

        assertNotNull(cfg.getConnectors());
        assertTrue(cfg.getConnectors().contains(m1));

        cfg.setConnectors(null);

        assertNull(cfg.getConnectors());

        assertTrue(cfg.withConnector(m1).getConnectors().contains(m1));

        cfg.withConnector(m2);

        assertTrue(cfg.getConnectors().contains(m1));
        assertTrue(cfg.getConnectors().contains(m2));
    }

    @Test
    public void testConfigProviders() {
        assertNull(cfg.getConfigProviders());

        NetworkConfigProvider c1 = () -> null;
        NetworkConfigProvider c2 = () -> null;

        cfg.setConfigProviders(Collections.singletonList(c1));

        assertNotNull(cfg.getConfigProviders());
        assertTrue(cfg.getConfigProviders().contains(c1));

        cfg.setConfigProviders(null);

        assertNull(cfg.getConfigProviders());

        assertTrue(cfg.withConfigProvider(c1).getConfigProviders().contains(c1));

        cfg.withConfigProvider(c2);

        assertTrue(cfg.getConfigProviders().contains(c1));
        assertTrue(cfg.getConfigProviders().contains(c2));
    }

    @Test
    public void testSsl() {
        assertNull(cfg.getSsl());

        NetworkSslConfig ssl = new NetworkSslConfig();

        cfg.setSsl(ssl);

        assertSame(ssl, cfg.getSsl());

        cfg.setSsl(null);

        assertNull(cfg.getSsl());

        assertSame(cfg, cfg.withSsl(ssl));
        assertSame(ssl, cfg.getSsl());
    }

}
