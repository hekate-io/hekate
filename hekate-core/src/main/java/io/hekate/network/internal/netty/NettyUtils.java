package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.Waiting;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Netty-related utilities.
 */
public final class NettyUtils {
    /** Time period (={@value}) in milliseconds for {@link #shutdown(EventExecutorGroup)} */
    // Package level for testing purposes.
    static final long GRACEFUL_SHUTDOWN_PERIOD = 0;

    private NettyUtils() {
        // No-op.
    }

    /**
     * {@link EventExecutorGroup#shutdownGracefully(long, long, TimeUnit) Shuts down} the specified executor with non default
     * ({@value #GRACEFUL_SHUTDOWN_PERIOD}) graceful shutdown period.
     *
     * @param executor Executor to shutdown (can be {@code null}).
     *
     * @return Waiting.
     */
    public static Waiting shutdown(EventExecutorGroup executor) {
        if (executor == null) {
            return Waiting.NO_WAIT;
        } else {
            Future<?> future = executor.shutdownGracefully(GRACEFUL_SHUTDOWN_PERIOD, Long.MAX_VALUE, TimeUnit.MILLISECONDS);

            return future::await;
        }
    }
}
