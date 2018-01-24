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

import io.hekate.metrics.local.CounterConfig;
import io.hekate.metrics.local.CounterMetric;
import io.hekate.metrics.local.LocalMetricsService;

class MessagingMetrics {
    private final CounterMetric requests;

    private final CounterMetric retry;

    private final CounterMetric tasks;

    private final CounterMetric tasksActive;

    public MessagingMetrics(String channelName, LocalMetricsService metrics) {
        assert channelName != null : "Channel name is null.";
        assert metrics != null : "Metrics service is null.";

        requests = metrics.register(new CounterConfig(channelName + ".messaging.requests.active"));

        tasksActive = metrics.register(new CounterConfig(channelName + ".messaging.tasks.active"));

        tasks = metrics.register(new CounterConfig()
            .withName(channelName + ".messaging.tasks.interim")
            .withTotalName(channelName + ".messaging.tasks.total")
            .withAutoReset(true)
        );

        retry = metrics.register(new CounterConfig()
            .withName(channelName + ".messaging.retry.interim")
            .withTotalName(channelName + ".messaging.retry.total")
            .withAutoReset(true)
        );
    }

    public void onPendingRequestsRemoved(int i) {
        requests.subtract(i);
    }

    public void onPendingRequestAdded() {
        requests.increment();
    }

    public void onAsyncEnqueue() {
        tasksActive.increment();

        tasks.increment();
    }

    public void onAsyncDequeue() {
        tasksActive.decrement();
    }

    public void onRetry() {
        retry.increment();
    }
}
