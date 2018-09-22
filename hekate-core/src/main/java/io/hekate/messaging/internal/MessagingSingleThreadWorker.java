/*
 * Copyright 2018 The Hekate Project
 *
 * The Hekate Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.hekate.messaging.internal;

import io.hekate.util.async.AsyncUtils;
import io.hekate.util.async.Waiting;
import io.hekate.util.format.ToString;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

class MessagingSingleThreadWorker implements MessagingWorker {
    private final ThreadPoolExecutor executor;

    private final ScheduledExecutorService timer;

    public MessagingSingleThreadWorker(ThreadFactory factory, ScheduledExecutorService timer) {
        assert timer != null : "Timer is null.";

        this.timer = timer;

        this.executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(), factory);
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
            AsyncUtils.fallbackExecutor().execute(task);
        }
    }

    @Override
    public Future<?> executeDeferred(long delay, Runnable task) {
        return timer.schedule(() -> execute(task), delay, TimeUnit.MILLISECONDS);
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
