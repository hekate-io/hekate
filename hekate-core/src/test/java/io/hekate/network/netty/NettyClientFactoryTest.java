package io.hekate.network.netty;

import io.hekate.HekateTestBase;
import io.hekate.codec.JdkCodecFactory;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import io.netty.handler.ssl.SslContext;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NettyClientFactoryTest extends HekateTestBase {
    private final NettyClientFactory<Object> factory = new NettyClientFactory<>();

    @Test
    public void testEventLoop() {
        EventLoopGroup eventLoop = mock(EventLoopGroup.class);

        assertNull(factory.getEventLoop());

        factory.setEventLoop(eventLoop);

        assertSame(eventLoop, factory.getEventLoop());

        factory.setEventLoop(null);

        assertNull(factory.getEventLoop());

        assertSame(factory, factory.withEventLoop(eventLoop));
        assertSame(eventLoop, factory.getEventLoop());
    }

    @Test
    public void testProtocol() {
        assertNull(factory.getProtocol());

        factory.setProtocol("test");

        assertEquals("test", factory.getProtocol());

        assertSame(factory, factory.withProtocol("test2"));

        assertEquals("test2", factory.getProtocol());
    }

    @Test
    public void testCodecFactory() throws Exception {
        assertNull(factory.getCodecFactory());

        JdkCodecFactory<Object> factory1 = new JdkCodecFactory<>();
        JdkCodecFactory<Object> factory2 = new JdkCodecFactory<>();

        factory.setCodecFactory(factory1);

        assertSame(factory1, factory.getCodecFactory());

        assertSame(factory, factory.withCodecFactory(factory2));

        assertSame(factory2, factory.getCodecFactory());
    }

    @Test
    public void testIdleTimeout() throws Exception {
        assertEquals(0, factory.getIdleTimeout());

        factory.setIdleTimeout(1000);

        assertEquals(1000, factory.getIdleTimeout());

        assertSame(factory, factory.withIdleTimeout(2000));

        assertEquals(2000, factory.getIdleTimeout());
    }

    @Test
    public void testSsl() {
        SslContext ctx = mock(SslContext.class);

        when(ctx.isClient()).thenReturn(true);

        assertNull(factory.getSsl());

        factory.setSsl(ctx);

        assertSame(ctx, factory.getSsl());

        factory.setSsl(null);

        assertNull(factory.getSsl());

        assertSame(factory, factory.withSsl(ctx));
        assertSame(ctx, factory.getSsl());
    }

    @Test
    public void testMetricsSink() {
        NettyMetricsSink metrics = mock(NettyMetricsSink.class);

        assertNull(factory.getMetrics());

        factory.setMetrics(metrics);

        assertSame(metrics, factory.getMetrics());

        factory.setMetrics(null);

        assertNull(factory.getMetrics());

        assertSame(factory, factory.withMetrics(metrics));
        assertSame(metrics, factory.getMetrics());
    }

    @Test
    public void testConnectTimeout() {
        assertNull(factory.getConnectTimeout());

        factory.setConnectTimeout(100);

        assertEquals(100, factory.getConnectTimeout().intValue());

        factory.setConnectTimeout(null);

        assertNull(factory.getConnectTimeout());

        assertSame(factory, factory.withConnectTimeout(1000));
        assertEquals(1000, factory.getConnectTimeout().intValue());
    }

    @Test
    public void testTcpNoDelay() {
        assertFalse(factory.getTcpNoDelay());

        factory.setTcpNoDelay(true);

        assertTrue(factory.getTcpNoDelay());

        factory.setTcpNoDelay(false);

        assertFalse(factory.getTcpNoDelay());

        assertSame(factory, factory.withTcpNoDelay(true));
        assertTrue(factory.getTcpNoDelay());
    }

    @Test
    public void testTcpReceiveBufferSize() {
        assertNull(factory.getSoReceiveBufferSize());

        factory.setSoReceiveBufferSize(1000);

        assertEquals(1000, factory.getSoReceiveBufferSize().intValue());

        factory.setSoReceiveBufferSize(null);

        assertNull(factory.getSoReceiveBufferSize());

        assertSame(factory, factory.withSoReceiveBufferSize(100));
        assertEquals(100, factory.getSoReceiveBufferSize().intValue());
    }

    @Test
    public void testTcpSendBufferSize() {
        assertNull(factory.getSoSendBufferSize());

        factory.setSoSendBufferSize(1000);

        assertEquals(1000, factory.getSoSendBufferSize().intValue());

        factory.setSoSendBufferSize(null);

        assertNull(factory.getSoSendBufferSize());

        assertSame(factory, factory.withSoSendBufferSize(1001));
        assertEquals(1001, factory.getSoSendBufferSize().intValue());
    }

    @Test
    public void testTcpReuseAddress() {
        assertNull(factory.getSoReuseAddress());

        factory.setSoReuseAddress(true);

        assertTrue(factory.getSoReuseAddress());

        factory.setSoReuseAddress(false);

        assertFalse(factory.getSoReuseAddress());

        factory.setSoReuseAddress(null);

        assertNull(factory.getSoReuseAddress());

        assertSame(factory, factory.withSoReuseAddress(true));
        assertTrue(factory.getSoReuseAddress());
    }

    @Test
    public void testLoggerCategory() {
        assertNull(factory.getLoggerCategory());

        factory.setLoggerCategory("test");

        assertEquals("test", factory.getLoggerCategory());

        assertSame(factory, factory.withLoggerCategory("test2"));

        assertEquals("test2", factory.getLoggerCategory());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(factory), factory.toString());
    }
}
