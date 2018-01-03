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

package io.hekate.cluster.internal;

import io.hekate.cluster.ClusterFilter;
import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterChangeEvent;
import io.hekate.cluster.event.ClusterEvent;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.cluster.event.ClusterJoinEvent;
import io.hekate.cluster.event.ClusterLeaveEvent;
import io.hekate.core.HekateException;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

class FilteredClusterView implements ClusterView {
    private static class FilteredListener implements ClusterEventListener {
        private final ClusterFilter filter;

        private final ClusterEventListener delegate;

        private final Set<ClusterEventType> eventTypes;

        public FilteredListener(ClusterFilter filter, ClusterEventListener delegate, Set<ClusterEventType> eventTypes) {
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

                        delegate.onEvent(new ClusterChangeEvent(topology, added, removed, event.hekate()));

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

            while (unwrapped instanceof FilteredListener) {
                unwrapped = ((FilteredListener)unwrapped).delegate;
            }

            return unwrapped;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof FilteredListener)) {
                return false;
            }

            FilteredListener other = (FilteredListener)o;

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

    @ToStringIgnore
    private final ClusterView parent;

    @ToStringIgnore
    private final ClusterFilter filter;

    private ClusterTopology topologyCache;

    public FilteredClusterView(ClusterView parent, ClusterFilter filter) {
        assert parent != null : "Parent view is null.";
        assert filter != null : "Filter is null.";

        this.filter = filter;
        this.parent = parent;
    }

    @Override
    public ClusterView filterAll(ClusterFilter newFilter) {
        ArgAssert.notNull(newFilter, "Filter");

        return new FilteredClusterView(this, newFilter);
    }

    @Override
    public ClusterTopology topology() {
        ClusterTopology parentTopology = parent.topology();

        ClusterTopology cached = this.topologyCache;

        if (cached == null || cached.version() < parentTopology.version()) {
            ClusterTopology newFiltered = parentTopology.filterAll(filter);

            this.topologyCache = newFiltered;

            cached = newFiltered;
        }

        return cached;
    }

    @Override
    public void addListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        parent.addListener(new FilteredListener(filter, listener, Collections.emptySet()));
    }

    @Override
    public void addListener(ClusterEventListener listener, ClusterEventType... eventTypes) {
        ArgAssert.notNull(listener, "Listener");

        Set<ClusterEventType> eventTypesSet;

        if (eventTypes != null && eventTypes.length > 0) {
            eventTypesSet = EnumSet.copyOf(Arrays.asList(eventTypes));
        } else {
            eventTypesSet = Collections.emptySet();
        }

        parent.addListener(new FilteredListener(filter, listener, eventTypesSet));
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        parent.removeListener(new FilteredListener(filter, listener, Collections.emptySet()));
    }

    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        return parent.futureOf(topology -> predicate.test(topology.filterAll(filter)));
    }

    @Override
    public String toString() {
        // Update cache.
        topology();

        return ToString.format(this);
    }
}
