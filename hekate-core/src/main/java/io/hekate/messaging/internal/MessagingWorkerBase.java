package io.hekate.messaging.internal;

import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import java.util.concurrent.ThreadPoolExecutor;

abstract class MessagingWorkerBase implements MessagingWorker {
    private final ThreadPoolExecutor executor;

    public MessagingWorkerBase(ThreadPoolExecutor executor) {
        assert executor != null : "Executor is null.";

        executor.setRejectedExecutionHandler((rejected, pool) ->
            AsyncUtils.fallbackExecutor().execute(rejected)
        );

        this.executor = executor;
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Override
    public void execute(Runnable task) {
        executor.execute(task);
    }

    @Override
    public int activeTasks() {
        return executor.getQueue().size();
    }

    @Override
    public long completedTasks() {
        return executor.getCompletedTaskCount();
    }

    public Waiting terminate() {
        return AsyncUtils.shutdown(executor);
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
