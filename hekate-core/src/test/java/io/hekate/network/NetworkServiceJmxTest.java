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

import io.hekate.HekateNodeParamTestBase;
import io.hekate.HekateTestContext;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NetworkServiceJmxTest extends HekateNodeParamTestBase {
    public NetworkServiceJmxTest(HekateTestContext ctx) {
        super(ctx);
    }

    @Test
    public void test() throws Exception {
        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(NetworkServiceFactory.class, net -> {
                net.withConnectTimeout(100500);
                net.withHeartbeatInterval(100501);
                net.withHeartbeatLossThreshold(100502);
                net.withNioThreads(2);
                net.withTransport(context().transport());
                net.withSsl(context().ssl().orElse(null));
                net.withTcpReceiveBufferSize(1024);
                net.withTcpSendBufferSize(2048);
                net.withTcpReuseAddress(true);
            });
        }).join();

        ObjectName name = node.get(JmxService.class).nameFor(NetworkServiceJmx.class);

        assertEquals(100500, (int)jmxAttribute(name, "ConnectTimeout", Integer.class, node));
        assertEquals(100501, (int)jmxAttribute(name, "HeartbeatInterval", Integer.class, node));
        assertEquals(100502, (int)jmxAttribute(name, "HeartbeatLossThreshold", Integer.class, node));
        assertEquals(2, (int)jmxAttribute(name, "NioThreads", Integer.class, node));
        assertEquals(context().transport().name(), jmxAttribute(name, "Transport", String.class, node));
        assertEquals(context().ssl().isPresent(), jmxAttribute(name, "Ssl", Boolean.class, node));
        assertTrue(jmxAttribute(name, "TcpNoDelay", Boolean.class, node));
        assertTrue(jmxAttribute(name, "TcpReuseAddress", Boolean.class, node));
        assertEquals(1024, (int)jmxAttribute(name, "TcpReceiveBufferSize", Integer.class, node));
        assertEquals(2048, (int)jmxAttribute(name, "TcpSendBufferSize", Integer.class, node));
    }
}
