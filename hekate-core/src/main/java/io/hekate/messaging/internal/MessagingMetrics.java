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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.atomic.LongAdder;

class MessagingMetrics {
    private final LongAdder reqAct = new LongAdder();

    private final Counter reqCount;

    private final LongAdder taskAct = new LongAdder();

    private final Counter taskCount;

    private final Counter retry;

    public MessagingMetrics(String channelName, MeterRegistry metrics) {
        assert channelName != null : "Channel name is null.";
        assert metrics != null : "Meter registry is null.";

        taskCount = Counter.builder("hekate.message.task.count")
            .tag("channel", channelName)
            .register(metrics);

        Gauge.builder("hekate.message.task.active", reqAct, LongAdder::doubleValue)
            .tag("channel", channelName)
            .register(metrics);

        retry = Counter.builder("hekate.message.retry")
            .tag("channel", channelName)
            .register(metrics);

        reqCount = Counter.builder("hekate.message.count")
            .tag("channel", channelName)
            .register(metrics);

        Gauge.builder("hekate.message.request.pending", reqAct, LongAdder::doubleValue)
            .tag("channel", channelName)
            .register(metrics);
    }

    public void onPendingRequestsRemoved(int i) {
        reqAct.add(-i);
    }

    public void onPendingRequestAdded() {
        reqCount.increment();

        reqAct.add(1);
    }

    public void onAsyncEnqueue() {
        taskCount.increment();

        taskAct.increment();
    }

    public void onAsyncDequeue() {
        taskAct.decrement();
    }

    public void onRetry() {
        retry.increment();
    }
}
