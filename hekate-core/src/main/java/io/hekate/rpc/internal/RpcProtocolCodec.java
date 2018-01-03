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

import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.rpc.internal.RpcProtocol.CompactCallRequest;
import io.hekate.rpc.internal.RpcProtocol.CompactSplitCallRequest;
import io.hekate.rpc.internal.RpcProtocol.ErrorResponse;
import io.hekate.rpc.internal.RpcProtocol.NullResponse;
import io.hekate.rpc.internal.RpcProtocol.ObjectResponse;
import io.hekate.rpc.internal.RpcProtocol.Type;
import java.io.IOException;

class RpcProtocolCodec implements Codec<RpcProtocol> {
    private static final Type[] TYPES_CACHE = Type.values();

    private final Codec<Object> delegate;

    public RpcProtocolCodec(Codec<Object> delegate) {
        assert delegate != null : "Delegate codec is null.";

        this.delegate = delegate;
    }

    @Override
    public boolean isStateful() {
        return delegate.isStateful();
    }

    @Override
    public Class<RpcProtocol> baseType() {
        return RpcProtocol.class;
    }

    @Override
    public void encode(RpcProtocol msg, DataWriter out) throws IOException {
        Type type = msg.type();

        out.writeByte(type.ordinal());

        switch (type) {
            case COMPACT_CALL_REQUEST: {
                CompactCallRequest request = (CompactCallRequest)msg;

                out.writeVarInt(request.methodIdx());

                delegate.encode(request.args(), out);

                break;
            }
            case COMPACT_SPLIT_CALL_REQUEST: {
                CompactSplitCallRequest request = (CompactSplitCallRequest)msg;

                out.writeVarInt(request.methodIdx());

                delegate.encode(request.args(), out);

                break;
            }
            case OBJECT_RESPONSE: {
                ObjectResponse response = (ObjectResponse)msg;

                delegate.encode(response.object(), out);

                break;
            }
            case NULL_RESPONSE: {
                // No-op.

                break;
            }
            case ERROR_RESPONSE: {
                ErrorResponse response = (ErrorResponse)msg;

                delegate.encode(response.cause(), out);

                break;
            }
            case CALL_REQUEST:
            case SPLIT_CALL_REQUEST:
            default: {
                throw new IllegalArgumentException("Unsupported message type: " + msg);
            }
        }
    }

    @Override
    public RpcProtocol decode(DataReader in) throws IOException {
        Type type = TYPES_CACHE[in.readByte()];

        switch (type) {
            case COMPACT_CALL_REQUEST: {
                int methodIdx = in.readVarInt();

                Object[] args = (Object[])delegate.decode(in);

                return new CompactCallRequest(methodIdx, args);
            }
            case COMPACT_SPLIT_CALL_REQUEST: {
                int methodIdx = in.readVarInt();

                Object[] args = (Object[])delegate.decode(in);

                return new CompactSplitCallRequest(methodIdx, args);
            }
            case OBJECT_RESPONSE: {
                Object obj = delegate.decode(in);

                return new ObjectResponse(obj);
            }
            case NULL_RESPONSE: {
                return NullResponse.INSTANCE;
            }
            case ERROR_RESPONSE: {
                Throwable cause = (Throwable)delegate.decode(in);

                return new ErrorResponse(cause);
            }
            case CALL_REQUEST:
            case SPLIT_CALL_REQUEST:
            default: {
                throw new IllegalArgumentException("Unsupported message type: " + type);
            }
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
