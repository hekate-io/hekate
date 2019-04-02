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

package io.hekate.cluster;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static io.hekate.core.jmx.JmxTestUtils.verifyJmxNode;
import static io.hekate.core.jmx.JmxTestUtils.verifyJmxTopology;
import static org.junit.Assert.assertEquals;

public class ClusterServiceJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        HekateTestNode node1 = createNode(boot -> {
            boot.withRole("role1");
            boot.withProperty("prop1", "value1");
            boot.withService(JmxServiceFactory.class, jmx -> jmx.withDomain("test-node-1"));
        }).join();

        HekateTestNode node2 = createNode(boot -> {
            boot.withRole("role2");
            boot.withProperty("prop2", "value2");
            boot.withService(JmxServiceFactory.class, jmx -> jmx.withDomain("test-node-2"));
        }).join();

        awaitForTopology(node1, node2);

        ObjectName clusterName1 = node1.get(JmxService.class).nameFor(ClusterServiceJmx.class);
        ObjectName clusterName2 = node2.get(JmxService.class).nameFor(ClusterServiceJmx.class);

        assertEquals(node1.cluster().topology().size(), (int)jmxAttribute(clusterName1, "TopologySize", Integer.class, node1));
        assertEquals(node2.cluster().topology().size(), (int)jmxAttribute(clusterName2, "TopologySize", Integer.class, node2));

        assertEquals(node1.cluster().topology().version(), (long)jmxAttribute(clusterName1, "TopologyVersion", Long.class, node1));
        assertEquals(node2.cluster().topology().version(), (long)jmxAttribute(clusterName2, "TopologyVersion", Long.class, node2));

        verifyJmxNode(node1.cluster().localNode(), jmxAttribute(clusterName1, "LocalNode", CompositeData.class, node1));
        verifyJmxNode(node2.cluster().localNode(), jmxAttribute(clusterName2, "LocalNode", CompositeData.class, node2));

        CompositeData[] jmxTop1 = jmxAttribute(clusterName1, "Topology", CompositeData[].class, node1);
        CompositeData[] jmxTop2 = jmxAttribute(clusterName1, "Topology", CompositeData[].class, node2);

        verifyJmxTopology(node1.cluster().topology(), jmxTop1);
        verifyJmxTopology(node2.cluster().topology(), jmxTop2);
    }
}
