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

package io.hekate.core.jmx;

import io.hekate.cluster.ClusterNode;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.HekateTestNode;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularDataSupport;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public final class JmxTestUtils {
    private static final String[] EMPTY_STRINGS = new String[0];

    private JmxTestUtils() {
        // No-op.
    }

    public static <T> T jmxAttribute(ObjectName name, String attr, Class<T> attrType, HekateTestNode node) throws Exception {
        return attrType.cast(node.get(JmxService.class).server().getAttribute(name, attr));
    }

    public static void verifyJmxTopology(ClusterTopology top, CompositeData[] jmxTop) {
        ClusterNode[] nodes = top.nodes().toArray(new ClusterNode[top.size()]);

        for (int i = 0; i < nodes.length; i++) {
            verifyJmxNode(nodes[i], jmxTop[i]);
        }
    }

    public static void verifyJmxNode(ClusterNode node, CompositeData jmx) {
        assertEquals(node.address().host(), jmx.get("host"));
        assertEquals(node.address().port(), jmx.get("port"));
        assertEquals(node.address().id().toString(), jmx.get("id"));
        assertEquals(node.name(), jmx.get("name"));
        assertArrayEquals(node.roles().toArray(EMPTY_STRINGS), (Object[])jmx.get("roles"));

        TabularDataSupport jmxProps = (TabularDataSupport)jmx.get("properties");

        node.properties().forEach((k, v) ->
            assertEquals(v, jmxProps.get(new String[]{k}).get("value"))
        );
    }
}
