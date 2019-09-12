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

package io.hekate.rpc.internal;

import io.hekate.cluster.ClusterNode;
import io.hekate.messaging.operation.FailureResponse;
import io.hekate.rpc.RpcInterfaceInfo;
import io.hekate.rpc.RpcMethodInfo;
import io.hekate.rpc.RpcRequest;
import io.hekate.util.format.ToString;
import io.hekate.util.format.ToStringIgnore;
import io.hekate.util.trace.TraceInfo;
import io.hekate.util.trace.Traceable;
import java.lang.reflect.Method;

abstract class RpcProtocol implements Traceable {
    enum Type {
        CALL_REQUEST,

        SPLIT_CALL_REQUEST,

        COMPACT_CALL_REQUEST,

        COMPACT_SPLIT_CALL_REQUEST,

        OBJECT_RESPONSE,

        NULL_RESPONSE,

        ERROR_RESPONSE,
    }

    static class RpcCall<T> extends RpcProtocol implements RpcRequest {
        private final String methodIdxKey;

        private final RpcInterfaceInfo<T> type;

        private final String tag;

        private final RpcMethodInfo method;

        @ToStringIgnore
        private final boolean split;

        @ToStringIgnore
        private final Object[] args;

        public RpcCall(String methodIdxKey, RpcInterfaceInfo<T> type, String tag, RpcMethodInfo method, Object[] args) {
            this(methodIdxKey, type, tag, method, args, false);
        }

        public RpcCall(String methodIdxKey, RpcInterfaceInfo<T> type, String tag, RpcMethodInfo method, Object[] args, boolean split) {
            this.methodIdxKey = methodIdxKey;
            this.type = type;
            this.tag = tag;
            this.method = method;
            this.args = args;
            this.split = split;
        }

        public String methodIdxKey() {
            return methodIdxKey;
        }

        @Override
        public String rpcTag() {
            return tag;
        }

        @Override
        public Class<T> rpcInterface() {
            return type.javaType();
        }

        @Override
        public int rpcVersion() {
            return type.version();
        }

        @Override
        public Method method() {
            return method.javaMethod();
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

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of(rpcInterface().getName() + '/' + method().getName())
                .withTag("tag", tag)
                .withTag("split", split)
                .withTag("args", args == null ? 0 : args.length);
        }
    }

    static class RpcCompactCall extends RpcProtocol {
        private final int methodIdx;

        @ToStringIgnore
        private final Object[] args;

        public RpcCompactCall(int methodIdx, Object[] args) {
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

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("compact-call/" + methodIdx)
                .withTag("args", args == null ? null : args.length);
        }
    }

    static class RpcCompactSplitCall extends RpcCompactCall {
        public RpcCompactSplitCall(int methodIdx, Object[] args) {
            super(methodIdx, args);
        }

        @Override
        public Type type() {
            return Type.COMPACT_SPLIT_CALL_REQUEST;
        }
    }

    static class RpcCallResult extends RpcProtocol {
        private final Object result;

        public RpcCallResult(Object result) {
            this.result = result;
        }

        public Object result() {
            return result;
        }

        @Override
        public Type type() {
            return Type.OBJECT_RESPONSE;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("object-result");
        }
    }

    static final class RpcCallNullResult extends RpcProtocol {
        static final RpcCallNullResult INSTANCE = new RpcCallNullResult();

        private RpcCallNullResult() {
            // No-op.
        }

        @Override
        public Type type() {
            return Type.NULL_RESPONSE;
        }

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("null-result");
        }
    }

    static class RpcCallError extends RpcProtocol implements FailureResponse {
        private final Throwable cause;

        public RpcCallError(Throwable cause) {
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

        @Override
        public TraceInfo traceInfo() {
            return TraceInfo.of("error")
                .withTag("error", cause);
        }
    }

    public abstract Type type();

    @Override
    public String toString() {
        return ToString.format(this);
    }
}
