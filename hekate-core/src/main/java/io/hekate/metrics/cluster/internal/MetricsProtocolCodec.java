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
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.metrics.MetricValue;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MetricsProtocolCodec implements Codec<MetricsProtocol> {

    private static final MetricsProtocol.Type[] TYPES_CACHE = MetricsProtocol.Type.values();

    private final Map<String, Integer> writeDict = new HashMap<>();

    private final Map<Integer, String> readDict = new HashMap<>();

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public Class<MetricsProtocol> baseType() {
        return MetricsProtocol.class;
    }

    @Override
    public void encode(MetricsProtocol msg, DataWriter out) throws IOException {
        MetricsProtocol.Type type = msg.type();

        out.writeByte(type.ordinal());

        CodecUtils.writeNodeId(msg.from(), out);

        switch (type) {
            case UPDATE_REQUEST: {
                MetricsProtocol.UpdateRequest updateMsg = (MetricsProtocol.UpdateRequest)msg;

                out.writeVarLong(updateMsg.targetVer());

                List<MetricsUpdate> updates = updateMsg.updates();

                encodeMetricUpdates(out, updates);

                break;
            }
            case UPDATE_RESPONSE: {
                MetricsProtocol.UpdateResponse requestMsg = (MetricsProtocol.UpdateResponse)msg;

                List<MetricsUpdate> updates = requestMsg.metrics();

                encodeMetricUpdates(out, updates);

                break;
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    @Override
    public MetricsProtocol decode(DataReader in) throws IOException {
        MetricsProtocol.Type type = TYPES_CACHE[in.readByte()];

        ClusterNodeId from = CodecUtils.readNodeId(in);

        switch (type) {
            case UPDATE_REQUEST: {
                long targetVer = in.readVarLong();

                List<MetricsUpdate> updates = decodeMetricUpdates(in);

                return new MetricsProtocol.UpdateRequest(from, targetVer, updates);
            }
            case UPDATE_RESPONSE: {
                List<MetricsUpdate> updates = decodeMetricUpdates(in);

                return new MetricsProtocol.UpdateResponse(from, updates);
            }
            default: {
                throw new IllegalArgumentException("Unexpected message type: " + type);
            }
        }
    }

    private List<MetricsUpdate> decodeMetricUpdates(DataReader in) throws IOException {
        List<MetricsUpdate> updates = null;

        int updatesSize = in.readVarIntUnsigned();

        if (updatesSize > 0) {
            updates = new ArrayList<>(updatesSize);

            for (int i = 0; i < updatesSize; i++) {
                // Node.
                ClusterNodeId node = CodecUtils.readNodeId(in);

                // Metrics version.
                long ver = in.readVarLong();

                // Metrics.
                int size = in.readVarIntUnsigned();

                Map<String, MetricValue> metrics = new HashMap<>(size, 1.0f);

                for (int j = 0; j < size; j++) {
                    int key = in.readVarInt();

                    String name;

                    if (key < 0) {
                        name = in.readUTF();

                        readDict.put(-key, name);
                    } else {
                        name = readDict.get(key);
                    }

                    assert name != null;

                    long val = in.readVarLong();

                    metrics.put(name, new MetricValue(name, val));
                }

                MetricsUpdate update = new MetricsUpdate(node, ver, metrics);

                updates.add(update);
            }
        }

        return updates;
    }

    private void encodeMetricUpdates(DataWriter out, List<MetricsUpdate> updates) throws IOException {
        if (updates == null || updates.isEmpty()) {
            out.writeVarIntUnsigned(0);
        } else {
            out.writeVarIntUnsigned(updates.size());

            for (MetricsUpdate update : updates) {
                // Note.
                CodecUtils.writeNodeId(update.node(), out);

                // Metrics version.
                out.writeVarLong(update.version());

                // Metrics.
                Map<String, MetricValue> metrics = update.metrics();

                out.writeVarIntUnsigned(metrics.size());

                for (MetricValue metric : metrics.values()) {
                    String name = metric.name();

                    Integer key = writeDict.get(name);

                    if (key == null) {
                        key = writeDict.size() + 1;

                        writeDict.put(name, key);

                        out.writeVarInt(-key);
                        out.writeUTF(name);
                    } else {
                        out.writeVarInt(key);
                    }

                    out.writeVarLong(metric.value());
                }
            }
        }
    }
}
