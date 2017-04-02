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

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

class SyncAffinityExecutor implements AffinityExecutor {
    private final DefaultAffinityWorker worker;

    public SyncAffinityExecutor(ThreadFactory threadFactory) {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, threadFactory);

        executor.setRemoveOnCancelPolicy(true);

        worker = new DefaultAffinityWorker(threadFactory);
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
        worker.execute(worker::shutdown);
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        worker.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }

    @Override
    public int getThreadPoolSize() {
        return 0;
    }
}
