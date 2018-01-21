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

import io.hekate.metrics.MetricsSource;

/**
 * A point in time snapshot of aggregated metrics.
 *
 * <p>
 * Such snapshots are calculated once per {@link LocalMetricsServiceFactory#setRefreshInterval(long)} and can be accessed via {@link
 * LocalMetricsService#snapshot()}.
 * </p>
 *
 * @see LocalMetricsService
 */
public interface MetricsSnapshot extends MetricsSource {
    /**
     * Returns the sequence number of this snapshot (starting with 0).
     *
     * @return Sequence number of this event.
     */
    int tick();
}
