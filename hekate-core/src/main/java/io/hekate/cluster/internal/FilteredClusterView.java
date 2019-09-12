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
import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import io.hekate.cluster.event.ClusterEventListener;
import io.hekate.cluster.event.ClusterEventType;
import io.hekate.core.internal.util.ArgAssert;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public class FilteredClusterView implements ClusterView {
    @ToStringIgnore
    private final ClusterView parent;

    @ToStringIgnore
    private final ClusterFilter filter;

    private ClusterTopology topology;

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

        ClusterTopology cached = this.topology;

        if (cached == null || cached.version() < parentTopology.version()) {
            this.topology = cached = parentTopology.filterAll(filter);
        }

        return cached;
    }

    @Override
    public void addListener(ClusterEventListener listener) {
        ArgAssert.notNull(listener, "Listener");

        parent.addListener(new FilteredClusterListener(filter, listener, Collections.emptySet()));
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

        parent.addListener(new FilteredClusterListener(filter, listener, eventTypesSet));
    }

    @Override
    public void removeListener(ClusterEventListener listener) {
        parent.removeListener(new FilteredClusterListener(filter, listener, Collections.emptySet()));
    }

    @Override
    public CompletableFuture<ClusterTopology> futureOf(Predicate<ClusterTopology> predicate) {
        return parent.futureOf(topology -> predicate.test(topology.filterAll(filter)));
    }

    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate) {
        return parent.awaitFor(topology -> predicate.test(topology.filterAll(filter)));
    }

    @Override
    public boolean awaitFor(Predicate<ClusterTopology> predicate, long timeout, TimeUnit timeUnit) {
        return parent.awaitFor(topology -> predicate.test(topology.filterAll(filter)));
    }

    @Override
    public String toString() {
        // Update cache.
        topology();

        return ToString.format(this);
    }
}
