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

import io.hekate.core.internal.util.ArgAssert;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;

class AffinityExecutorAdaptor implements Executor {
    private final AffinityExecutor async;

    public AffinityExecutorAdaptor(AffinityExecutor async) {
        this.async = async;
    }

    @Override
    public void execute(Runnable command) {
        async.workerFor(ThreadLocalRandom.current().nextInt()).execute(command);
    }

    public Executor getExecutor(Object affinityKey) {
        ArgAssert.notNull(affinityKey, "Affinity key");

        return async.workerFor(affinityKey.hashCode());
    }
}
