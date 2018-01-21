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

import io.hekate.HekateTestBase;
import io.hekate.cluster.ClusterNodeId;
import io.hekate.codec.StreamDataReader;
import io.hekate.codec.StreamDataWriter;
import io.hekate.metrics.MetricValue;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateRequest;
import io.hekate.metrics.cluster.internal.MetricsProtocol.UpdateResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MetricsProtocolCodecTest extends HekateTestBase {
    private final MetricsProtocolCodec codec = new MetricsProtocolCodec();

    @Test
    public void testResponsePushBackUpdatesOnly() throws Exception {
        repeat(3, i -> {
            ClusterNodeId from = newNodeId();

            List<MetricsUpdate> updates = newMetricUpdates();

            UpdateResponse response = new UpdateResponse(from, updates);

            UpdateResponse decoded = encodeDecode(response);

            assertEquals(from, decoded.from());

            assertEqualsMetricUpdates(updates, decoded.metrics());
        });
    }

    @Test
    public void testRequestWithoutDictionary() throws Exception {
        repeat(3, i -> {
            List<MetricsUpdate> updates = newMetricUpdates();

            UpdateRequest request = new UpdateRequest(newNodeId(), i, updates);

            UpdateRequest decoded = encodeDecode(request);

            assertEquals(request.from(), decoded.from());
            assertEquals(i, decoded.targetVer());

            assertEqualsMetricUpdates(updates, decoded.updates());
        });
    }

    private void assertEqualsMetricUpdates(List<MetricsUpdate> updates1, List<MetricsUpdate> updates2) {
        assertNotNull(updates1);
        assertNotNull(updates2);
        assertEquals(updates1.size(), updates2.size());

        for (int i = 0; i < updates1.size(); i++) {
            MetricsUpdate u1 = updates1.get(i);
            MetricsUpdate u2 = updates2.get(i);

            assertEquals(u1.node(), u2.node());
            assertEquals(u1.version(), u2.version());

            assertEquals(u1.metrics().size(), u2.metrics().size());

            assertEqualsUpdates(u1, u2);
            assertEqualsUpdates(u2, u1);
        }
    }

    private void assertEqualsUpdates(MetricsUpdate u1, MetricsUpdate u2) {
        for (String name : u1.metrics().keySet()) {
            MetricValue m1 = u1.metrics().get(name);
            MetricValue m2 = u2.metrics().get(name);

            assertNotNull(m2);
            assertEquals(m1.name(), m2.name());
            assertEquals(m1.value(), m2.value());
        }
    }

    private List<MetricsUpdate> newMetricUpdates() {
        List<MetricsUpdate> updates = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            Map<String, MetricValue> metrics = new HashMap<>();

            for (int j = 0; j < 3; j++) {
                metrics.put("m" + j, new MetricValue("m" + j, j + 1));
            }

            updates.add(new MetricsUpdate(newNodeId(), i, metrics));
        }

        return updates;
    }

    private <T extends MetricsProtocol> T encodeDecode(T msg) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        StreamDataWriter out = new StreamDataWriter(bout);

        codec.encode(msg, out);

        ByteArrayInputStream bin = new ByteArrayInputStream(bout.toByteArray());

        StreamDataReader in = new StreamDataReader(bin);

        @SuppressWarnings("unchecked")
        T result = (T)codec.decode(in);

        return result;
    }
}
