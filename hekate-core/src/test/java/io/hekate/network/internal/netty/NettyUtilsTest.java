package io.hekate.network.internal.netty;

import io.hekate.HekateTestBase;
import io.hekate.core.internal.util.Waiting;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NettyUtilsTest extends HekateTestBase {
    @Test
    public void testUtilityClass() throws Exception {
        assertValidUtilityClass(NettyUtils.class);
    }

    @Test
    public void testShutdown() throws Exception {
        assertSame(Waiting.NO_WAIT, NettyUtils.shutdown(null));

        EventExecutorGroup mock = mock(EventExecutorGroup.class);

        when(mock.shutdownGracefully(anyLong(), anyLong(), any())).thenReturn(genericMock(Future.class));

        NettyUtils.shutdown(mock);

        verify(mock.shutdownGracefully(eq(NettyUtils.GRACEFUL_SHUTDOWN_PERIOD), eq(Long.MAX_VALUE), same(TimeUnit.MILLISECONDS)));
    }

    @Test
    public void testRunAtAllCost() throws Exception {
        EventLoop eventLoop = new DefaultEventLoop();

        try {
            // Check execution on event loop thread.
            Exchanger<Boolean> exchanger = new Exchanger<>();

            NettyUtils.runAtAllCost(eventLoop, () -> {
                try {
                    exchanger.exchange(eventLoop.inEventLoop());
                } catch (InterruptedException e) {
                    // No-op.
                }
            });

            assertTrue(exchanger.exchange(null));

            // Check execution on fallback thread if event loop is terminated.
            NettyUtils.shutdown(eventLoop).awaitUninterruptedly();

            NettyUtils.runAtAllCost(eventLoop, () -> {
                try {
                    exchanger.exchange(eventLoop.inEventLoop());
                } catch (InterruptedException e) {
                    // No-op.
                }
            });

            assertFalse(exchanger.exchange(null));
        } finally {
            NettyUtils.shutdown(eventLoop).awaitUninterruptedly();
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T genericMock(Class<? super T> classToMock) {
        return (T)mock(classToMock);
    }
}
