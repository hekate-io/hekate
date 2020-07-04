/*
 * Copyright 2020 The Hekate Project
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
import java.util.concurrent.ThreadPoolExecutor;

abstract class MessagingWorkerBase implements MessagingWorker {
    private final ThreadPoolExecutor executor;

    public MessagingWorkerBase(ThreadPoolExecutor executor) {
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
