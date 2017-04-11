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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

class AsyncAffinityExecutor implements AffinityExecutor {

    private final DefaultAffinityWorker[] workers;

    private final int size;

    public AsyncAffinityExecutor(ThreadFactory factory, int size, ScheduledExecutorService timer) {
        assert size > 0 : "Thread pool size must be above zero [size=" + size + ']';
        assert timer != null : "Timer is null.";

        this.size = size;

        workers = new DefaultAffinityWorker[size];

        for (int i = 0; i < size; i++) {
            workers[i] = new DefaultAffinityWorker(factory, timer);
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
        for (DefaultAffinityWorker worker : workers) {
            worker.execute(worker::shutdown);
        }
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        for (DefaultAffinityWorker worker : workers) {
            worker.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        }
    }

    @Override
    public int getThreadPoolSize() {
        return size;
    }
}
