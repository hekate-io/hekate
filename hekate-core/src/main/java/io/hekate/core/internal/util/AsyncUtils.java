package io.hekate.core.internal.util;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class AsyncUtils {
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

    public static Executor fallbackExecutor() {
        return FALLBACK_EXECUTOR;
    }

    public static Waiting shutdown(ExecutorService executor) {
        if (executor == null) {
            return Waiting.NO_WAIT;
        } else {
            executor.shutdown();

            return () -> executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        }
    }

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
