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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.util.format.ToString;
import io.hekate.util.trace.TraceInfo;
import io.hekate.util.trace.Traceable;

abstract class CoordinationProtocol implements Traceable {
    enum Type {
        REQUEST,

        RESPONSE,

        REJECT
    }

    static class Request extends CoordinationProtocol {
        private final String processName;

        private final ClusterNodeId from;

        private final ClusterHash topology;

        private final Object request;

        public Request(String processName, ClusterNodeId from, ClusterHash topology, Object request) {
            this.processName = processName;
            this.from = from;
            this.topology = topology;
            this.request = request;
        }

        public String processName() {
            return processName;
        }

        public ClusterNodeId from() {
            return from;
        }

        public ClusterHash topology() {
            return topology;
        }

        public Object request() {
            return request;
        }

        @Override
        public Type type() {
            return Type.REQUEST;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("/" + processName + "/" + request.getClass().getSimpleName())
                .withTag("topology-hash", topology);
        }
    }

    static class Response extends CoordinationProtocol {
        private final String processName;

        private final Object response;

        public Response(String processName, Object response) {
            this.processName = processName;
            this.response = response;
        }

        public String processName() {
            return processName;
        }

        public Object response() {
            return response;
        }

        @Override
        public Type type() {
            return Type.RESPONSE;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("ok");
        }
    }

    static final class Reject extends CoordinationProtocol {
        static final Reject INSTANCE = new Reject();

        private Reject() {
            // No-op.
        }

        @Override
        public Type type() {
            return Type.REJECT;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("reject");
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
