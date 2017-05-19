package io.hekate.network.internal.netty;

import io.hekate.core.internal.util.AsyncUtils;
import io.hekate.core.internal.util.Waiting;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Netty-related utilities.
 */
public final class NettyUtils {
    /** Time period (={@value}) in milliseconds for {@link #shutdown(EventExecutorGroup)}. */
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

    /**
     * Executes the task using the provided event loop or falls back to {@link AsyncUtils#fallbackExecutor()} if event loop is {@link
     * EventLoop#isShuttingDown() shutting down}.
     *
     * @param eventLoop Event loop.
     * @param task Task.
     */
    public static void runAtAllCost(EventLoop eventLoop, Runnable task) {
        assert eventLoop != null : "Event loop is null.";
        assert task != null : "Task is null.";

        boolean notified = false;

        // Try to execute via event loop.
        if (!eventLoop.isShuttingDown()) {
            try {
                eventLoop.execute(task);

                notified = true;
            } catch (RejectedExecutionException e) {
                // No-op.
            }
        }

        // If couldn't notify via event loop then use the fallback executor.
        if (!notified) {
            AsyncUtils.fallbackExecutor().execute(task);
        }
    }
}
