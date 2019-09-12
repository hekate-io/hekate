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

package io.hekate.cluster.health;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DefaultFailureDetectorJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        HekateTestNode node1 = createNode(boot -> boot.withService(JmxServiceFactory.class, jmx -> jmx.withDomain("test-node-1"))).join();
        HekateTestNode node2 = createNode(boot -> boot.withService(JmxServiceFactory.class, jmx -> jmx.withDomain("test-node-2"))).join();

        awaitForTopology(node1, node2);

        ObjectName name1 = node1.get(JmxService.class).nameFor(DefaultFailureDetectorJmx.class);
        ObjectName name2 = node2.get(JmxService.class).nameFor(DefaultFailureDetectorJmx.class);

        assertTrue(jmxAttribute(name1, "HeartbeatInterval", Long.class, node1) > 0);
        assertTrue(jmxAttribute(name2, "HeartbeatInterval", Long.class, node2) > 0);

        assertTrue(jmxAttribute(name1, "HeartbeatLossThreshold", Integer.class, node1) > 0);
        assertTrue(jmxAttribute(name2, "HeartbeatLossThreshold", Integer.class, node2) > 0);

        assertTrue(jmxAttribute(name1, "FailureDetectionQuorum", Integer.class, node1) > 0);
        assertTrue(jmxAttribute(name2, "FailureDetectionQuorum", Integer.class, node2) > 0);

        String[] monitored1 = jmxAttribute(name1, "MonitoredAddresses", String[].class, node1);
        String[] monitored2 = jmxAttribute(name2, "MonitoredAddresses", String[].class, node2);

        assertEquals(toSet(node2.localNode().address().toString()), toSet(monitored1));
        assertEquals(toSet(node1.localNode().address().toString()), toSet(monitored2));
    }
}
