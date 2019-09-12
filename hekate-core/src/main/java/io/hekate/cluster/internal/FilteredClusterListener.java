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

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.HekateException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

class FilteredClusterListener implements ClusterEventListener {
    private final ClusterFilter filter;

    private final ClusterEventListener delegate;

    private final Set<ClusterEventType> eventTypes;

    public FilteredClusterListener(ClusterFilter filter, ClusterEventListener delegate, Set<ClusterEventType> eventTypes) {
        assert filter != null : "Filter is null.";
        assert delegate != null : "Delegate is null.";
        assert eventTypes != null : "Event types are null.";

        this.filter = filter;
        this.delegate = delegate;
        this.eventTypes = eventTypes;
    }

    @Override
    public void onEvent(ClusterEvent event) throws HekateException {
        boolean notify = eventTypes.isEmpty() || eventTypes.contains(event.type());

        if (notify) {
            ClusterTopology topology = event.topology().filterAll(filter);

            switch (event.type()) {
                case JOIN: {
                    delegate.onEvent(new ClusterJoinEvent(topology, event.hekate()));

                    break;
                }
                case LEAVE: {
                    ClusterLeaveEvent leave = event.asLeave();

                    List<ClusterNode> added = filterToImmutable(leave.added());
                    List<ClusterNode> removed = filterToImmutable(leave.removed());

                    delegate.onEvent(new ClusterLeaveEvent(leave.reason(), topology, added, removed, event.hekate()));

                    break;
                }
                case CHANGE: {
                    ClusterChangeEvent change = event.asChange();

                    List<ClusterNode> added = filterToImmutable(change.added());
                    List<ClusterNode> removed = filterToImmutable(change.removed());
                    List<ClusterNode> failed = filterToImmutable(change.failed());

                    if (!added.isEmpty() || !removed.isEmpty()) {
                        delegate.onEvent(new ClusterChangeEvent(topology, added, removed, failed, event.hekate()));
                    }

                    break;
                }
                default: {
                    throw new IllegalArgumentException("Unexpected event type: " + event);
                }
            }
        }
    }

    private List<ClusterNode> filterToImmutable(List<ClusterNode> nodes) {
        List<ClusterNode> filtered = filter.apply(nodes);

        return filtered.isEmpty() ? Collections.emptyList() : Collections.unmodifiableList(nodes);
    }

    private ClusterEventListener unwrap() {
        ClusterEventListener unwrapped = delegate;

        while (unwrapped instanceof FilteredClusterListener) {
            unwrapped = ((FilteredClusterListener)unwrapped).delegate;
        }

        return unwrapped;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof FilteredClusterListener)) {
            return false;
        }

        FilteredClusterListener other = (FilteredClusterListener)o;

        return unwrap().equals(other.unwrap());
    }

    @Override
    public int hashCode() {
        return unwrap().hashCode();
    }

    @Override
    public String toString() {
        return unwrap().toString();
    }
}
