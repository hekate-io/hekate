/*
 * Copyright 2022 The Hekate Project
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

import io.hekate.cluster.ClusterTopology;
import io.hekate.cluster.ClusterView;
import java.util.function.Function;

/**
 * Cache for {@link ClusterView#topologyContext(Function)}.
 */
public class TopologyContextCache {
    /**
     * Cache entry.
     */
    private static class CacheEntry {
        /** Topology version. */
        private final long version;

        /** Cached value. */
        private final Object value;

        /**
         * Constructs a new instance.
         *
         * @param version Topology version.
         * @param value Cached value.
         */
        public CacheEntry(long version, Object value) {
            this.version = version;
            this.value = value;
        }

        /**
         * Topology version.
         *
         * @return Topology version.
         */
        public long version() {
            return version;
        }

        /**
         * Cached value.
         *
         * @return Cached value.
         */
        public Object value() {
            return value;
        }
    }

    /** Latest cached value. */
    private volatile CacheEntry ctx;

    /**
     * Constructs a new context object or returns a cached one.
     *
     * @param topology Topology.
     * @param supplier Context supplier function.
     * @param <C> Context object type.
     *
     * @return Context object.
     */
    @SuppressWarnings("unchecked")
    public <C> C get(ClusterTopology topology, Function<ClusterTopology, C> supplier) {
        // Volatile read.
        CacheEntry ctx = this.ctx;

        if (ctx == null || ctx.version() != topology.version()) {
            /////////////////////////////////////////////////////////////////////////////////////////////////////
            // While topology change is in progress it is possible that different threads will try to update
            // this field concurrently (some with the older topology version and some with a newer).
            // We do not guard against such operations since at some point in time all threads will converge
            // on the same topology version;
            // also, we expect context supplier function to be idempotent and free of side effects.
            /////////////////////////////////////////////////////////////////////////////////////////////////////
            // Volatile write.
            this.ctx = ctx = new CacheEntry(
                topology.version(),
                supplier.apply(topology)
            );
        }

        return (C)ctx.value();
    }
}
