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

package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.unicast.FailureResponse;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcRequest;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import java.lang.reflect.Method;

abstract class RpcProtocol {
    enum Type {
        CALL_REQUEST,

        SPLIT_CALL_REQUEST,

        COMPACT_CALL_REQUEST,

        COMPACT_SPLIT_CALL_REQUEST,

        OBJECT_RESPONSE,

        NULL_RESPONSE,

        ERROR_RESPONSE,
    }

    static class CallRequest<T> extends RpcProtocol implements RpcRequest {
        private final RpcInterfaceInfo<T> rpcType;

        private final RpcMethodInfo rpcMethod;

        private final String rpcTag;

        private final boolean split;

        @ToStringIgnore
        private final Object[] args;

        public CallRequest(RpcInterfaceInfo<T> rpcType, String rpcTag, RpcMethodInfo rpcMethod, Object[] args) {
            this(rpcType, rpcTag, rpcMethod, args, false);
        }

        public CallRequest(RpcInterfaceInfo<T> rpcType, String rpcTag, RpcMethodInfo rpcMethod, Object[] args, boolean split) {
            this.rpcType = rpcType;
            this.rpcTag = rpcTag;
            this.rpcMethod = rpcMethod;
            this.args = args;
            this.split = split;
        }

        public RpcMethodInfo rpcMethod() {
            return rpcMethod;
        }

        public RpcInterfaceInfo<T> rpcType() {
            return rpcType;
        }

        @Override
        public String rpcTag() {
            return rpcTag;
        }

        @Override
        public Class<T> rpcInterface() {
            return rpcType.javaType();
        }

        @Override
        public int rpcVersion() {
            return rpcType.version();
        }

        @Override
        public Method method() {
            return rpcMethod.javaMethod();
        }

        @Override
        public Object[] args() {
            return args;
        }

        @Override
        public boolean hasArgs() {
            return args != null;
        }

        @Override
        public boolean isSplit() {
            return split;
        }

        @Override
        public Type type() {
            return Type.CALL_REQUEST;
        }
    }

    static class CompactCallRequest extends RpcProtocol {
        private final int methodIdx;

        @ToStringIgnore
        private final Object[] args;

        public CompactCallRequest(int methodIdx, Object[] args) {
            this.methodIdx = methodIdx;
            this.args = args;
        }

        public int methodIdx() {
            return methodIdx;
        }

        public Object[] args() {
            return args;
        }

        @Override
        public Type type() {
            return Type.COMPACT_CALL_REQUEST;
        }
    }

    static class CompactSplitCallRequest extends CompactCallRequest {
        public CompactSplitCallRequest(int methodIdx, Object[] args) {
            super(methodIdx, args);
        }

        @Override
        public Type type() {
            return Type.COMPACT_SPLIT_CALL_REQUEST;
        }
    }

    static class ObjectResponse extends RpcProtocol {
        private final Object object;

        public ObjectResponse(Object object) {
            this.object = object;
        }

        public Object object() {
            return object;
        }

        @Override
        public Type type() {
            return Type.OBJECT_RESPONSE;
        }
    }

    static final class NullResponse extends RpcProtocol {
        static final NullResponse INSTANCE = new NullResponse();

        private NullResponse() {
            // No-op.
        }

        @Override
        public Type type() {
            return Type.NULL_RESPONSE;
        }
    }

    static class ErrorResponse extends RpcProtocol implements FailureResponse {
        private final Throwable cause;

        public ErrorResponse(Throwable cause) {
            this.cause = cause;
        }

        public Throwable cause() {
            return cause;
        }

        @Override
        public Type type() {
            return Type.ERROR_RESPONSE;
        }

        @Override
        public Throwable asError(ClusterNode fromNode) {
            return cause;
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
