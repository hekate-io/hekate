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

package io.hekate.cluster.internal;

import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;

class DeferredClusterListener {
    private final ClusterEventListener listener;

    private final ClusterEventType[] eventTypes;

    public DeferredClusterListener(ClusterEventListener listener, ClusterEventType[] eventTypes) {
        this.listener = listener;
        this.eventTypes = eventTypes;
    }

    public ClusterEventListener listener() {
        return listener;
    }

    public ClusterEventType[] eventTypes() {
        return eventTypes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof DeferredClusterListener)) {
            return false;
        }

        DeferredClusterListener that = (DeferredClusterListener)o;

        return listener.equals(that.listener);
    }

    @Override
    public int hashCode() {
        return listener.hashCode();
    }
}
