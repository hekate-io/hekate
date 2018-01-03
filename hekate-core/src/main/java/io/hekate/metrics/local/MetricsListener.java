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

package io.hekate.metrics.local;

import io.hekate.core.Hekate;

/**
 * Metric listener.
 *
 * <p>
 * Instances of this interface can be registered within the {@link LocalMetricsService} in order to get notifications about metric updates.
 * Listeners can be registered either {@link LocalMetricsServiceFactory#withListener(MetricsListener) statically} or {@link
 * LocalMetricsService#addListener(MetricsListener) dynamically}. Statically registered listeners survive {@link Hekate} node
 * restarts, while dynamically registered listeners will be automatically unregistered when node gets terminated.
 * </p>
 *
 * <p>
 * Listeners gets notified every {@link LocalMetricsServiceFactory#getRefreshInterval()} right after the {@link LocalMetricsService}
 * finishes gathering latest values from {@link CounterMetric counters} and {@link Probe probes}.
 * </p>
 *
 * <p>
 * <b>Note:</b> listeners are notified on the same thread that performs metrics management and hence should process notifications as fast
 * as possible. If notification processing is a time consuming task then it is highly recommended to offload such task to some other thread
 * and process it asynchronously.
 * </p>
 *
 * @see LocalMetricsService#addListener(MetricsListener)
 * @see LocalMetricsServiceFactory#withListener(MetricsListener)
 */
@FunctionalInterface
public interface MetricsListener {
    /**
     * Notifies this listener on metrics update event.
     *
     * <p>
     * <b>Note:</b> listeners are notified on the same thread that performs metrics management and hence should process notifications as
     * fast as possible. If notification processing is a time consuming task then it is highly recommended to offload such task to some
     * other thread and process it asynchronously.
     * </p>
     *
     * @param event Update event.
     */
    void onUpdate(MetricsUpdateEvent event);
}
