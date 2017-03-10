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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

class SyncAffinityExecutor implements AffinityExecutor {
    private final ScheduledExecutorService scheduler;

    private final AffinityWorker worker;

    public SyncAffinityExecutor(ThreadFactory threadFactory) {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadFactory);

        worker = new AffinityWorker() {
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
        };

        scheduler = executor;
    }

    @Override
    public AffinityWorker workerFor(int affinity) {
        return worker;
    }

    @Override
    public boolean isAsync() {
        return false;
    }

    @Override
    public void terminate() {
        scheduler.execute(scheduler::shutdown);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        scheduler.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    @Override
    public int getThreadPoolSize() {
        return 0;
    }
}
