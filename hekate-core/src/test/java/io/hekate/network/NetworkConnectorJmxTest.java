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

package io.hekate.network;

import io.hekate.HekateNodeTestBase;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkConnectorJmxTest extends HekateNodeTestBase {
    @Test
    public void test() throws Exception {
        HekateTestNode node = createNode(boot -> {
                boot.withService(JmxServiceFactory.class);
                boot.withService(NetworkServiceFactory.class, net ->
                    net.withConnector(new NetworkConnectorConfig<>()
                        .withProtocol("test.jmx")
                        .withIdleSocketTimeout(100500)
                        .withLogCategory("io.hekate.test.jmx")
                        .withNioThreads(3)
                        .withServerHandler((msg, from) -> {
                            // No-op.
                        })
                    )
                );
            }
        ).join();

        ObjectName name = node.get(JmxService.class).nameFor(NetworkConnectorJmx.class, "test.jmx");

        assertEquals("test.jmx", jmxAttribute(name, "Protocol", String.class, node));
        assertEquals(100500, (long)jmxAttribute(name, "IdleSocketTimeout", Long.class, node));
        assertEquals("io.hekate.test.jmx", jmxAttribute(name, "LogCategory", String.class, node));
        assertEquals(3, (int)jmxAttribute(name, "NioThreads", Integer.class, node));
        assertTrue(jmxAttribute(name, "Server", Boolean.class, node));
    }
}
