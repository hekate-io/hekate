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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterUuid;
import io.hekate.util.format.ToString;

abstract class CoordinationProtocol {
    enum Type {
        REQUEST,

        RESPONSE,

        REJECT
    }

    static class Request extends CoordinationProtocol {
        private final String processName;

        private final ClusterUuid from;

        private final ClusterHash topology;

        private final Object request;

        public Request(String processName, ClusterUuid from, ClusterHash topology, Object request) {
            this.processName = processName;
            this.from = from;
            this.topology = topology;
            this.request = request;
        }

        public String getProcessName() {
            return processName;
        }

        public ClusterUuid getFrom() {
            return from;
        }

        public ClusterHash getTopology() {
            return topology;
        }

        public Object getRequest() {
            return request;
        }

        @Override
        public Type getType() {
            return Type.REQUEST;
        }
    }

    static class Response extends CoordinationProtocol {
        private final String processName;

        private final Object response;

        public Response(String processName, Object response) {
            this.processName = processName;
            this.response = response;
        }

        public String getProcessName() {
            return processName;
        }

        public Object getResponse() {
            return response;
        }

        @Override
        public Type getType() {
            return Type.RESPONSE;
        }
    }

    static final class Reject extends CoordinationProtocol {
        static final Reject INSTANCE = new Reject();

        private Reject() {
            // No-op.
        }

        @Override
        public Type getType() {
            return Type.REJECT;
        }
    }

    public abstract Type getType();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
