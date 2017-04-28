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

package io.hekate.metrics.cluster.internal;

import io.hekate.cluster.ClusterUuid;
import io.hekate.codec.Codec;
import io.hekate.codec.CodecUtils;
import io.hekate.codec.DataReader;
import io.hekate.codec.DataWriter;
import io.hekate.metrics.local.internal.StaticMetric;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MetricsProtocolCodec implements Codec<MetricsProtocol> {
    public static final String PROTOCOL_ID = "hekate.metrics";

    private static final MetricsProtocol.Type[] TYPES_CACHE = MetricsProtocol.Type.values();

    private final Map<String, Integer> writeDict = new HashMap<>();

    private final Map<Integer, String> readDict = new HashMap<>();

    @Override
    public boolean isStateful() {
        return true;
    }

    @Override
    public void encode(MetricsProtocol msg, DataWriter out) throws IOException {
        MetricsProtocol.Type type = msg.getType();

        out.writeByte(type.ordinal());

        CodecUtils.writeNodeId(msg.getFrom(), out);

        switch (type) {
            case UPDATE_REQUEST: {
                MetricsProtocol.UpdateRequest updateMsg = (MetricsProtocol.UpdateRequest)msg;

                out.writeLong(updateMsg.getTargetVer());

                List<MetricsUpdate> updates = updateMsg.getUpdates();

                encodeMetricUpdates(out, updates);

                break;
            }
            case UPDATE_RESPONSE: {
                MetricsProtocol.UpdateResponse requestMsg = (MetricsProtocol.UpdateResponse)msg;

                List<MetricsUpdate> updates = requestMsg.getMetrics();

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

        ClusterUuid from = CodecUtils.readNodeId(in);

        switch (type) {
            case UPDATE_REQUEST: {
                long targetVer = in.readLong();

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

        int updatesSize = in.readInt();

        if (updatesSize > 0) {
            updates = new ArrayList<>(updatesSize);

            for (int i = 0; i < updatesSize; i++) {
                // Node.
                ClusterUuid node = CodecUtils.readNodeId(in);

                // Metrics version.
                long ver = in.readLong();

                // Metrics.
                int size = in.readInt();

                Map<String, StaticMetric> metrics = new HashMap<>(size, 1.0f);

                for (int j = 0; j < size; j++) {
                    int key = in.readInt();

                    String name;

                    if (key < 0) {
                        name = in.readUTF();

                        readDict.put(-key, name);
                    } else {
                        name = readDict.get(key);
                    }

                    assert name != null;

                    long val = in.readLong();

                    metrics.put(name, new StaticMetric(name, val));
                }

                MetricsUpdate update = new MetricsUpdate(node, ver, metrics);

                updates.add(update);
            }
        }

        return updates;
    }

    private void encodeMetricUpdates(DataWriter out, List<MetricsUpdate> updates) throws IOException {
        if (updates == null || updates.isEmpty()) {
            out.writeInt(0);
        } else {
            out.writeInt(updates.size());

            for (MetricsUpdate update : updates) {
                // Note.
                CodecUtils.writeNodeId(update.getNode(), out);

                // Metrics version.
                out.writeLong(update.getVersion());

                // Metrics.
                Map<String, StaticMetric> metrics = update.getMetrics();

                out.writeInt(metrics.size());

                for (StaticMetric metric : metrics.values()) {
                    String name = metric.getName();

                    Integer key = writeDict.get(name);

                    if (key == null) {
                        key = writeDict.size() + 1;

                        writeDict.put(name, key);

                        out.writeInt(-key);
                        out.writeUTF(name);
                    } else {
                        out.writeInt(key);
                    }

                    out.writeLong(metric.getValue());
                }
            }
        }
    }
}
