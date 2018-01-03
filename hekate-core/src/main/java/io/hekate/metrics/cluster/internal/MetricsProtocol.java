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

package io.hekate.metrics.cluster.internal;

import io.hekate.cluster.ClusterNodeId;
import io.hekate.util.format.ToString;
import java.util.List;

abstract class MetricsProtocol {
    enum Type {
        UPDATE_REQUEST,

        UPDATE_RESPONSE
    }

    static class UpdateRequest extends MetricsProtocol {
        private final long targetVer;

        private final List<MetricsUpdate> updates;

        public UpdateRequest(ClusterNodeId from, long targetVer, List<MetricsUpdate> updates) {
            super(from);

            this.targetVer = targetVer;
            this.updates = updates;
        }

        public long targetVer() {
            return targetVer;
        }

        public List<MetricsUpdate> updates() {
            return updates;
        }

        @Override
        public Type type() {
            return Type.UPDATE_REQUEST;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    static class UpdateResponse extends MetricsProtocol {
        private final List<MetricsUpdate> metrics;

        public UpdateResponse(ClusterNodeId from, List<MetricsUpdate> metrics) {
            super(from);
            this.metrics = metrics;
        }

        public List<MetricsUpdate> metrics() {
            return metrics;
        }

        @Override
        public Type type() {
            return Type.UPDATE_RESPONSE;
        }

        @Override
        public String toString() {
            return ToString.format(this);
        }
    }

    private final ClusterNodeId from;

    public MetricsProtocol(ClusterNodeId from) {
        this.from = from;
    }

    public abstract Type type();

    public ClusterNodeId from() {
        return from;
    }
}
