package io.hekate.network.netty;

import io.hekate.HekateTestBase;
import io.hekate.util.format.ToString;
import io.netty.channel.EventLoopGroup;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

public class NettyServerHandlerConfigTest extends HekateTestBase {
    private final NettyServerHandlerConfig<?> cfg = new NettyServerHandlerConfig<>();

    @Test
    public void testEventLoop() {
        EventLoopGroup eventLoop = mock(EventLoopGroup.class);

        assertNull(cfg.getEventLoop());

        cfg.setEventLoop(eventLoop);

        assertSame(eventLoop, cfg.getEventLoop());

        cfg.setEventLoop(null);

        assertNull(cfg.getEventLoop());

        assertSame(cfg, cfg.withEventLoop(eventLoop));
        assertSame(eventLoop, cfg.getEventLoop());
    }

    @Test
    public void testToString() {
        assertEquals(ToString.format(cfg), cfg.toString());
    }
}
