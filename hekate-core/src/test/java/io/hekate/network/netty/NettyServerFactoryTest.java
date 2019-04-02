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

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import org.junit.Test;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class NettyServerFactoryTest extends HekateTestBase {
    private final NettyServerFactory factory = new NettyServerFactory();

    @Test
    public void testDisableHeartbeats() {
        assertFalse(factory.isDisableHeartbeats());

        factory.setDisableHeartbeats(true);

        assertTrue(factory.isDisableHeartbeats());

        assertSame(factory, factory.withDisableHeartbeats(false));
        assertFalse(factory.isDisableHeartbeats());
    }

    @Test
    public void testAcceptorEventLoop() {
        EventLoopGroup eventLoop = mock(EventLoopGroup.class);

        assertNull(factory.getAcceptorEventLoop());

        factory.setAcceptorEventLoop(eventLoop);

        assertSame(eventLoop, factory.getAcceptorEventLoop());

        factory.setAcceptorEventLoop(null);

        assertNull(factory.getAcceptorEventLoop());

        assertSame(factory, factory.withAcceptorEventLoop(eventLoop));
        assertSame(eventLoop, factory.getAcceptorEventLoop());
    }

    @Test
    public void testWorkerEventLoop() {
        EventLoopGroup eventLoop = mock(EventLoopGroup.class);

        assertNull(factory.getWorkerEventLoop());

        factory.setWorkerEventLoop(eventLoop);

        assertSame(eventLoop, factory.getWorkerEventLoop());

        factory.setWorkerEventLoop(null);

        assertNull(factory.getWorkerEventLoop());

        assertSame(factory, factory.withWorkerEventLoop(eventLoop));
        assertSame(eventLoop, factory.getWorkerEventLoop());
    }

    @Test
    public void testHandlers() throws Exception {
        NettyServerHandlerConfig<Object> h1 = new NettyServerHandlerConfig<>();
        NettyServerHandlerConfig<Object> h2 = new NettyServerHandlerConfig<>();

        assertNull(factory.getHandlers());

        factory.setHandlers(asList(h1, h2));

        assertEquals(2, factory.getHandlers().size());
        assertSame(h1, factory.getHandlers().get(0));
        assertSame(h2, factory.getHandlers().get(1));

        factory.setHandlers(null);

        assertNull(factory.getHandlers());

        assertSame(factory, factory.withHandler(h1));
        assertEquals(1, factory.getHandlers().size());
        assertSame(h1, factory.getHandlers().get(0));
    }

    @Test
    public void testSsl() {
        SslContext ctx = mock(SslContext.class);

        assertNull(factory.getSsl());

        factory.setSsl(ctx);

        assertSame(ctx, factory.getSsl());

        factory.setSsl(null);

        assertNull(factory.getSsl());

        assertSame(factory, factory.withSsl(ctx));
        assertSame(ctx, factory.getSsl());
    }

    @Test
    public void testMetricsFactory() {
        NettyMetricsFactory metrics = mock(NettyMetricsFactory.class);

        assertNull(factory.getMetrics());

        factory.setMetrics(metrics);

        assertSame(metrics, factory.getMetrics());

        factory.setMetrics(null);

        assertNull(factory.getMetrics());

        assertSame(factory, factory.withMetrics(metrics));
        assertSame(metrics, factory.getMetrics());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
