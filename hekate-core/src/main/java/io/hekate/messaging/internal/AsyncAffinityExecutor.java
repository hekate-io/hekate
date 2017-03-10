/*
 * Copyright 2017 The Hekate Project
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

import io.hekate.core.internal.util.Utils;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

class AsyncAffinityExecutor implements AffinityExecutor {
    private static class AffinityWorkerImpl implements AffinityWorker {
        private final ScheduledExecutorService executor;

        public AffinityWorkerImpl(ScheduledExecutorService executor) {
            this.executor = executor;
        }

        @Override
        public void execute(Runnable task) {
            executor.execute(task);
        }

        @Override
        public void executeDeferred(long delay, Runnable task) {
            executor.schedule(task, delay, TimeUnit.MILLISECONDS);
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

    private final AffinityWorkerImpl[] workers;

    private final int size;

    public AsyncAffinityExecutor(ThreadFactory threadFactory, int size) {
        assert size > 0 : "Thread pool size must be above zero [size=" + size + ']';

        this.size = size;

        workers = new AffinityWorkerImpl[size];

        for (int i = 0; i < size; i++) {
            ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);

            workers[i] = new AffinityWorkerImpl(executor);
        }
    }

    @Override
    public AffinityWorker workerFor(int affinity) {
        return workers[Utils.mod(affinity, size)];
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Override
    public void terminate() {
        for (AffinityWorkerImpl worker : workers) {
            worker.execute(worker::shutdown);
        }
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        for (AffinityWorkerImpl worker : workers) {
            worker.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public int getThreadPoolSize() {
        return size;
    }
}
