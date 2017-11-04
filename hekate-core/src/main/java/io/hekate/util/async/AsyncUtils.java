package io.hekate.util.async;

import io.hekate.core.internal.util.ArgAssert;
import io.hekate.core.internal.util.HekateThreadFactory;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Set of utility methods for asynchronous tasks processing.
 */
public final class AsyncUtils {
    /** See {@link #fallbackExecutor()}. */
    static final ThreadPoolExecutor FALLBACK_EXECUTOR;

    static {
        HekateThreadFactory factory = new HekateThreadFactory("AsyncFallback") {
            @Override
            protected String resolveNodeName(String nodeName) {
                return null;
            }
        };

        BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        FALLBACK_EXECUTOR = new ThreadPoolExecutor(0, 1, 1, TimeUnit.SECONDS, queue, r -> {
            Thread t = factory.newThread(r);

            t.setDaemon(true);

            return t;
        });

        FALLBACK_EXECUTOR.allowCoreThreadTimeOut(true);
    }

    private AsyncUtils() {
        // No-op.
    }

    /**
     * Fallback executor.
     *
     * <p>
     * Returns a single-threaded executor instance that can be used run tasks that couldn't be executed on their own executors for some
     * reasons. This executor should not be used for execution of a regular flow tasks.
     * </p>
     *
     * @return Fallback executor.
     */
    public static Executor fallbackExecutor() {
        return FALLBACK_EXECUTOR;
    }

    /**
     * {@link ExecutorService#shutdown() Shuts down} the specified executor service and returns a {@link Waiting} that will {@link
     * Waiting#await() await} for {@link ExecutorService#awaitTermination(long, TimeUnit) termination}.
     *
     * @param executor Executor service to shut down.
     *
     * @return Waiting for {@link ExecutorService#awaitTermination(long, TimeUnit) termination}.
     */
    public static Waiting shutdown(ExecutorService executor) {
        if (executor == null) {
            return Waiting.NO_WAIT;
        } else {
            executor.shutdown();

            return () -> executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Uninterruptedly waits for the specified {@link Future} to complete, and then returns its result.
     *
     * @param future Future.
     * @param <T> Result type.
     *
     * @return Result of the {@link Future#get()} method.
     *
     * @throws ExecutionException If thrown by the {@link Future#get()} method.
     */
    public static <T> T getUninterruptedly(Future<T> future) throws ExecutionException {
        ArgAssert.notNull(future, "future");

        boolean interrupted = false;

        try {
            while (true) {
                try {
                    return future.get();
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
