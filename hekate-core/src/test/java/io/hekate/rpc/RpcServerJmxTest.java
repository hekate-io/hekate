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

package io.hekate.rpc;

import io.hekate.HekateNodeTestBase;
import io.hekate.cluster.ClusterTopology;
import io.hekate.core.internal.HekateTestNode;
import io.hekate.core.jmx.JmxService;
import io.hekate.core.jmx.JmxServiceFactory;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.Test;

import static io.hekate.core.jmx.JmxTestUtils.jmxAttribute;
import static io.hekate.core.jmx.JmxTestUtils.verifyJmxTopology;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class RpcServerJmxTest extends HekateNodeTestBase {
    @Rpc(version = 2, minClientVersion = 1)
    public interface TestRpc1 {
        @SuppressWarnings("unused")
        String testMethod1();
    }

    @Rpc(version = 2, minClientVersion = 1)
    public interface TestRpc2 {
        @SuppressWarnings("unused")
        String testMethod2();
    }

    public static class TestRpc implements TestRpc1, TestRpc2 {
        @Override
        public String testMethod1() {
            return "one";
        }

        @Override
        public String testMethod2() {
            return "two";
        }
    }

    @Test
    public void test() throws Exception {
        HekateTestNode node = createNode(boot -> {
            boot.withService(JmxServiceFactory.class);
            boot.withService(RpcServiceFactory.class, rpc -> {
                rpc.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpc1.class))
                );
                rpc.withServer(new RpcServerConfig()
                    .withHandler(mock(TestRpc2.class))
                );
                rpc.withServer(new RpcServerConfig()
                    .withHandler(new TestRpc())
                    .withTag("tag-1")
                );
                rpc.withServer(new RpcServerConfig()
                    .withHandler(new TestRpc())
                    .withTag("tag-2")
                    .withTag("tag-3")
                );
            });
        }).join();

        for (RpcServerInfo server : node.rpc().servers()) {
            for (RpcInterfaceInfo<?> rpcFace : server.interfaces()) {
                if (server.tags().isEmpty()) {
                    verifyRpcJmx(rpcFace, null, server, node);
                } else {
                    for (String tag : server.tags()) {
                        verifyRpcJmx(rpcFace, tag, server, node);
                    }
                }
            }
        }
    }

    private void verifyRpcJmx(RpcInterfaceInfo<?> rpcFace, String tag, RpcServerInfo server, HekateTestNode node) throws Exception {
        ObjectName name;

        if (tag == null) {
            name = node.get(JmxService.class).nameFor(RpcServerJmx.class, rpcFace.name());
        } else {
            name = node.get(JmxService.class).nameFor(RpcServerJmx.class, rpcFace.name() + '#' + tag);
        }

        assertEquals(server.rpc().getClass().getName(), jmxAttribute(name, "HandlerType", String.class, node));
        assertTrue((int)jmxAttribute(name, "InterfaceVersion", Integer.class, node) > 0);
        assertTrue((int)jmxAttribute(name, "InterfaceMinClientVersion", Integer.class, node) > 0);

        ClusterTopology topology = node.rpc().clusterOf(rpcFace.javaType(), tag).topology();

        assertEquals(1, topology.size());
        verifyJmxTopology(topology, jmxAttribute(name, "Topology", CompositeData[].class, node));
    }
}
