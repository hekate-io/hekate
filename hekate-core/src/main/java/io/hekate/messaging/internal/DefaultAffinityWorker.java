package io.hekate.messaging.internal;

import io.hekate.core.internal.util.Utils;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class DefaultAffinityWorker implements AffinityWorker {
    private final ThreadPoolExecutor executor;

    private final ScheduledExecutorService timer;

    public DefaultAffinityWorker(ThreadFactory factory, ScheduledExecutorService timer) {
        assert timer != null : "Timer is null.";

        this.timer = timer;

        LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();

        executor = new ThreadPoolExecutor(0, 1, Long.MAX_VALUE, TimeUnit.MILLISECONDS, queue, factory);
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

    @Override
    public boolean isShutdown() {
        return executor.isShutdown();
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        executor.awaitTermination(timeout, unit);
    }
}
