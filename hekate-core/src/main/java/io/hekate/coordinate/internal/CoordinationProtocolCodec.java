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

package io.hekate.coordinate.internal;

import io.hekate.cluster.ClusterHash;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecFactory;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

class CoordinationProtocolCodec implements Codec<CoordinationProtocol> {
    private static final CoordinationProtocol.Type[] TYPES_CACHE = CoordinationProtocol.Type.values();

    private final Map<String, CodecFactory<Object>> codecFactories;

    private final Map<String, Codec<Object>> codecs = new HashMap<>();

    public CoordinationProtocolCodec(Map<String, CodecFactory<Object>> codecFactories) {
        this.codecFactories = codecFactories;
    }

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public Class<CoordinationProtocol> baseType() {
        return CoordinationProtocol.class;
    }

    @Override
    public void encode(CoordinationProtocol msg, DataWriter out) throws IOException {
        CoordinationProtocol.Type type = msg.type();

        out.writeByte(type.ordinal());

        switch (type) {
            case PREPARE: {
                CoordinationProtocol.Prepare request = (CoordinationProtocol.Prepare)msg;

                String process = request.processName();

                out.writeUTF(process);

                CodecUtils.writeNodeId(request.from(), out);
                CodecUtils.writeTopologyHash(request.topology(), out);

                break;
            }
            case REQUEST: {
                CoordinationProtocol.Request request = (CoordinationProtocol.Request)msg;

                String process = request.processName();

                out.writeUTF(process);

                CodecUtils.writeNodeId(request.from(), out);
                CodecUtils.writeTopologyHash(request.topology(), out);

                codecFor(process).encode(request.request(), out);

                break;
            }
            case RESPONSE: {
                CoordinationProtocol.Response response = (CoordinationProtocol.Response)msg;

                String process = response.processName();

                out.writeUTF(process);

                codecFor(process).encode(response.response(), out);

                break;
            }
            case CONFIRM:
            case REJECT: {
                // No-op.

                break;
            }
            case COMPLETE: {
                CoordinationProtocol.Complete request = (CoordinationProtocol.Complete)msg;

                String process = request.processName();

                out.writeUTF(process);

                CodecUtils.writeNodeId(request.from(), out);
                CodecUtils.writeTopologyHash(request.topology(), out);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + msg);
            }
        }
    }

    @Override
    public CoordinationProtocol decode(DataReader in) throws IOException {
        CoordinationProtocol.Type type = TYPES_CACHE[in.readByte()];

        switch (type) {
            case PREPARE: {
                String process = in.readUTF();

                ClusterNodeId from = CodecUtils.readNodeId(in);
                ClusterHash hash = CodecUtils.readTopologyHash(in);

                return new CoordinationProtocol.Prepare(process, from, hash);
            }
            case REQUEST: {
                String process = in.readUTF();

                ClusterNodeId from = CodecUtils.readNodeId(in);
                ClusterHash hash = CodecUtils.readTopologyHash(in);

                Object request = codecFor(process).decode(in);

                return new CoordinationProtocol.Request(process, from, hash, request);
            }
            case RESPONSE: {
                String process = in.readUTF();

                Object response = codecFor(process).decode(in);

                return new CoordinationProtocol.Response(process, response);
            }
            case REJECT: {
                return CoordinationProtocol.Reject.INSTANCE;
            }
            case CONFIRM: {
                return CoordinationProtocol.Confirm.INSTANCE;
            }
            case COMPLETE: {
                String process = in.readUTF();

                ClusterNodeId from = CodecUtils.readNodeId(in);
                ClusterHash hash = CodecUtils.readTopologyHash(in);

                return new CoordinationProtocol.Complete(process, from, hash);
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    private Codec<Object> codecFor(String process) {
        return codecs.computeIfAbsent(process, k ->
            codecFactories.get(process).createCodec()
        );
    }
}
