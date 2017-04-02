package io.hekate.messaging.internal;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

class DefaultAffinityWorker implements AffinityWorker {
    private final ScheduledThreadPoolExecutor executor;

    public DefaultAffinityWorker(ThreadFactory threadFactory) {
        executor = new ScheduledThreadPoolExecutor(1, threadFactory);

        executor.setRemoveOnCancelPolicy(true);
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
            ForkJoinPool.commonPool().execute(task);
        }
    }

    @Override
    public Future<?> executeDeferred(long delay, Runnable task) {
        return executor.schedule(task, delay, TimeUnit.MILLISECONDS);
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
