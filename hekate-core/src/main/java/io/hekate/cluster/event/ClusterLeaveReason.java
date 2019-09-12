/*
 * Copyright 2019 The Hekate Project
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

package io.hekate.cluster.event;

import io.hekate.cluster.split.SplitBrainDetector;
import io.hekate.core.Hekate;

/**
 * Reason of a {@link ClusterLeaveEvent}.
 *
 * @see ClusterLeaveEvent#reason()
 */
public enum ClusterLeaveReason {
    /** Leaving because of the {@link Hekate#leave()} method call. */
    LEAVE,

    /** Leaving because of the {@link Hekate#terminate()} method call. */
    TERMINATE,

    /** Leaving because of the cluster split-brain had been detected by the {@link SplitBrainDetector}. */
    SPLIT_BRAIN
}
