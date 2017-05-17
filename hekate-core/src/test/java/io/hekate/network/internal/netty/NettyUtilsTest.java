package io.hekate.network.internal.netty;

import io.hekate.HekateTestBase;
import io.hekate.core.internal.util.Waiting;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
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

        when(mock.shutdownGracefully(anyLong(), anyLong(), any(TimeUnit.class))).thenReturn(genericMock(Future.class));

        NettyUtils.shutdown(mock);

        verify(mock.shutdownGracefully(eq(NettyUtils.GRACEFUL_SHUTDOWN_PERIOD), eq(Long.MAX_VALUE), same(TimeUnit.MILLISECONDS)));
    }

    @SuppressWarnings("unchecked")
    private static <T> T genericMock(Class<? super T> classToMock) {
        return (T)mock(classToMock);
    }
}
