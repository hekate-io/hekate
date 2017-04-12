package io.hekate.messaging.internal;

import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class MessagingForkJoinWorker implements MessagingWorker {
    private final ExecutorService executor;

    private final ScheduledExecutorService timer;

    public MessagingForkJoinWorker(int parallelism, HekateThreadFactory factory, ScheduledExecutorService timer) {
        assert parallelism > 0 : "Parallelism must be above zero [parallelism=" + parallelism + ']';
        assert factory != null : "Thread Factory is null.";
        assert timer != null : "Timer is null.";

        this.timer = timer;

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

    @Override
    public Future<?> executeDeferred(long delay, Runnable task) {
        return timer.schedule(() -> execute(task), delay, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        executor.awaitTermination(timeout, unit);
    }
}
