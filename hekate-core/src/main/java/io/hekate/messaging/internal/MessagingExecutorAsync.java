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

import io.hekate.core.internal.util.HekateThreadFactory;
import io.hekate.core.internal.util.Utils;
import io.hekate.util.async.Waiting;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

class MessagingExecutorAsync implements MessagingExecutor {
    private final MessagingSingleThreadWorker[] affinityWorkers;

    private final MessagingThreadPoolWorker pooledWorker;

    private final int size;

    public MessagingExecutorAsync(HekateThreadFactory factory, int size, ScheduledExecutorService timer) {
        assert size > 0 : "Thread pool size must be above zero [size=" + size + ']';
        assert timer != null : "Timer is null.";

        this.size = size;

        affinityWorkers = new MessagingSingleThreadWorker[size];

        for (int i = 0; i < size; i++) {
            affinityWorkers[i] = new MessagingSingleThreadWorker(factory, timer);
        }

        pooledWorker = new MessagingThreadPoolWorker(size, factory, timer);
    }

    @Override
    public MessagingWorker workerFor(int affinity) {
        return affinityWorkers[Utils.mod(affinity, size)];
    }

    @Override
    public MessagingWorker pooledWorker() {
        return pooledWorker;
    }

    @Override
    public boolean isAsync() {
        return true;
    }

    @Override
    public Waiting terminate() {
        List<Waiting> waiting = new ArrayList<>();

        for (MessagingSingleThreadWorker worker : affinityWorkers) {
            waiting.add(worker.terminate());
        }

        waiting.add(pooledWorker.terminate());

        return Waiting.awaitAll(waiting);
    }

    @Override
    public int poolSize() {
        return size;
    }

    @Override
    public int activeTasks() {
        int size = 0;

        for (MessagingSingleThreadWorker worker : affinityWorkers) {
            size += worker.activeTasks();
        }

        return size + pooledWorker.activeTasks();
    }

    @Override
    public long completedTasks() {
        int size = 0;

        for (MessagingSingleThreadWorker worker : affinityWorkers) {
            size += worker.completedTasks();
        }

        return size + pooledWorker.completedTasks();
    }
}
