package io.hekate.messaging.internal;

import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

class ForkJoinMessagingWorker implements Executor {
    private final ExecutorService executor;

    public ForkJoinMessagingWorker(int parallelism, HekateThreadFactory factory) {
        assert parallelism > 0 : "Parallelism must be above zero [parallelism=" + parallelism + ']';
        assert factory != null : "Thread Factory is null.";

        executor = new ForkJoinPool(parallelism, factory, null, true);
    }

    @Override
    public void execute(Runnable task) {
        boolean fallback = false;

        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            fallback = true;
        }

        if (fallback) {
            Utils.fallbackExecutor().execute(task);
        }
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        executor.awaitTermination(timeout, unit);
    }
}
