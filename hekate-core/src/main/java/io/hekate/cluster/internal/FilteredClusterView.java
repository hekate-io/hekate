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
            boolean notify = eventTypes.isEmpty() || eventTypes.contains(event.getType());

            if (notify) {
                ClusterTopology topology = event.getTopology().filterAll(this.filter);

                switch (event.getType()) {
                    case JOIN: {
                        delegate.onEvent(new ClusterJoinEvent(event.getHekate(), topology));

                        break;
                    }
                    case LEAVE: {
                        ClusterLeaveEvent leave = event.asLeave();

                        Set<ClusterNode> added = filterToImmutable(leave.getAdded());
                        Set<ClusterNode> removed = filterToImmutable(leave.getRemoved());

                        delegate.onEvent(new ClusterLeaveEvent(event.getHekate(), topology, added, removed));

                        break;
                    }
                    case CHANGE: {
                        ClusterChangeEvent change = event.asChange();

                        Set<ClusterNode> added = filterToImmutable(change.getAdded());
                        Set<ClusterNode> removed = filterToImmutable(change.getRemoved());

                        delegate.onEvent(new ClusterChangeEvent(event.getHekate(), topology, added, removed));

                        break;
                    }
                    default: {
                        throw new IllegalArgumentException("Unexpected event type: " + event);
                    }
                }
            }
        }

        private Set<ClusterNode> filterToImmutable(Set<ClusterNode> nodes) {
            Set<ClusterNode> filtered = filter.apply(nodes);

            return filtered.isEmpty() ? Collections.emptySet() : Collections.unmodifiableSet(nodes);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (!(o instanceof FilteredListener)) {
                return false;
            }

            FilteredListener that = (FilteredListener)o;

            return delegate.equals(that.delegate);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }

    @ToStringIgnore
    private final ClusterView root;

    @ToStringIgnore
    private final ClusterFilter filter;

    private ClusterTopology topologyCache;

    public FilteredClusterView(ClusterView root, ClusterFilter filter) {
        assert root != null : "Root view is null.";
        assert filter != null : "Filter is null.";

        this.filter = filter;

        this.root = root;
    }

    @Override
    public ClusterView filterAll(ClusterFilter newFilter) {
        ArgAssert.notNull(newFilter, "Filter");

        return new FilteredClusterView(root, ClusterFilter.and(filter, newFilter));
    }

    @Override
    public ClusterTopology getTopology() {
        long realVer = root.getTopology().getVersion();

        ClusterTopology cached = this.topologyCache;

        if (cached == null || cached.getVersion() < realVer) {
            ClusterTopology newFiltered = root.getTopology().filterAll(filter);

            this.topologyCache = newFiltered;

            cached = newFiltered;
        }

        return cached;
    }

    @Override
    public void addListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        root.addListener(new FilteredListener(filter, listener, Collections.emptySet()));
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

        root.addListener(new FilteredListener(filter, listener, eventTypesSet));
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        root.removeListener(new FilteredListener(filter, listener, Collections.emptySet()));
    }

    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        return root.futureOf(topology -> predicate.test(topology.filterAll(filter)));
    }

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
