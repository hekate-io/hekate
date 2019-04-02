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

import io.hekate.codec.Codec;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.rpc.internal.RpcProtocol.RpcCallError;
import io.hekate.rpc.internal.RpcProtocol.RpcCallNullResult;
import io.hekate.rpc.internal.RpcProtocol.RpcCallResult;
import io.hekate.rpc.internal.RpcProtocol.RpcCompactCall;
import io.hekate.rpc.internal.RpcProtocol.RpcCompactSplitCall;
import io.hekate.rpc.internal.RpcProtocol.Type;
import io.hekate.util.format.ToString;
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
                RpcCompactCall request = (RpcCompactCall)msg;

                out.writeVarInt(request.methodIdx());

                delegate.encode(request.args(), out);

                break;
            }
            case COMPACT_SPLIT_CALL_REQUEST: {
                RpcCompactSplitCall request = (RpcCompactSplitCall)msg;

                out.writeVarInt(request.methodIdx());

                delegate.encode(request.args(), out);

                break;
            }
            case OBJECT_RESPONSE: {
                RpcCallResult response = (RpcCallResult)msg;

                delegate.encode(response.result(), out);

                break;
            }
            case NULL_RESPONSE: {
                // No-op.

                break;
            }
            case ERROR_RESPONSE: {
                RpcCallError response = (RpcCallError)msg;

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

                return new RpcCompactCall(methodIdx, args);
            }
            case COMPACT_SPLIT_CALL_REQUEST: {
                int methodIdx = in.readVarInt();

                Object[] args = (Object[])delegate.decode(in);

                return new RpcCompactSplitCall(methodIdx, args);
            }
            case OBJECT_RESPONSE: {
                Object obj = delegate.decode(in);

                return new RpcCallResult(obj);
            }
            case NULL_RESPONSE: {
                return RpcCallNullResult.INSTANCE;
            }
            case ERROR_RESPONSE: {
                Throwable cause = (Throwable)delegate.decode(in);

                return new RpcCallError(cause);
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
        return ToString.format(this);
    }
}
